/*
 * Fledge Dispatcher API class for dispatcher control request classes
 *
 * Copyright (c) 2021 Dianomic Systems
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Mark Riddoch, Massimiliano Pinto
 */
#include <automation.h>
#include <dispatcher_service.h>
#include <query.h>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <logger.h>

using namespace std;

/**
 * Destructor for the script. Cleanup any resources held
 * by the script.
 */
Script::~Script()
{
}

/**
 * Execute a script by iterating through the steps of the script 
 * calling the execute method of each step. As soon as the first step
 * fails then the entire script is aborted and fails.
 *
 * @param service	Pointer the the dispatcher service
 */
bool Script::execute(DispatcherService *service, const KVList& parameters)
{
	if (!m_loaded)
	{
		if (!load(service))
		{
			return false;
		}
	}
	Logger::getLogger()->debug("Execute script %s, Caller %s, type %s with parameters %s",
				m_name.c_str(),
				m_source_name.c_str(),
				m_source_type.c_str(),
				parameters.toString().c_str());

	int stepNo = 0;
	for (auto it = m_steps.begin(); it != m_steps.end(); ++it)
	{
		stepNo++;
		if (it->second)
		{
			bool res = it->second->execute(service, parameters);

			if (!res)
			{
				Logger::getLogger()->info("Execute of %s failed at step %d",
							m_name.c_str(),
							stepNo);
				return res;
			}
		}
		else
		{
			Logger::getLogger()->error("Script %s hsa an invalid step %d",
						m_name.c_str(),
						stepNo);
			return false;
		}
	}
	return true;
}

/**
 * Load the script from the database
 *
 * @param service	Pointer to the dispatcher service
 * @return bool		Return true if the script was succesfully loaded
 */
bool Script::load(DispatcherService *service)
{
	Logger *log = Logger::getLogger();

	log->debug("Loading script '%s' for service '%s', "
			"caller name '%s', type '%s', URL '%s'",
			m_name.c_str(),
			service->getName().c_str(),
			m_source_name.c_str(),
			m_source_type.c_str(),
			m_request_url.c_str());

	StorageClient *storage = service->getStorageClient();

	Where *where = new Where("name", Equals, m_name);
	Query scriptQuery(where);
	ResultSet *result = storage->queryTable(SCRIPT_TABLE, scriptQuery);
	if (!result || result->rowCount() != 1)
	{
		log->error("Unable to retrieve a control script called '%s'",
				m_name.c_str());
		if (result)
			delete result;
		return false;
	}

	ResultSet::RowIterator row = result->firstRow();
	ResultSet::ColumnValue *scriptCol;
	try
	{
		scriptCol = (*row)->getColumn("steps");
	}
	catch(ResultNoSuchColumnException *e)
	{
		log->error("Script '%s' does not have 'steps' column",
			m_name.c_str());
		delete e;
		if (result)
			delete result;
		return false;

	}
	catch(...)
	{
		log->error("Script '%s': found generic exception while fetching 'steps' column",
			m_name.c_str());
		if (result)
			delete result;
		return false;
	}

	// Data in "steps" might be string or JSON
	const rapidjson::Value *doc;
	rapidjson::Value stringV;

	// Check for string
	if (scriptCol->getType() == ColumnType::STRING_COLUMN)
	{
		char *str;
		str = scriptCol->getString();
		// Substitute singke quote with double quote to allow parsing
		char *p = str;
		while (*p)
		{
			if (*p == '\'')
				*p = '\"';
			p++;
		}

		Document jsonDoc;
		ParseResult ok = jsonDoc.Parse(str);
		if (!ok)
		{
			log->error("Parse error in script %s: %s (%u)",
					m_name.c_str(),
					GetParseError_En(ok.Code()),
					ok.Offset());
			log->error("Script %s is: %s", m_name.c_str(), str);
			delete result;
			return false;
		}

		if (jsonDoc.IsArray())
		{
			stringV = jsonDoc.GetArray();
			doc = &stringV;
		}
	}
	// Check for JSON
	else if (scriptCol->getType() == ColumnType::JSON_COLUMN)
	{
		doc = scriptCol->getJSON();
	}
	// Unsupported data type
	else
	{
		log->error("Control script '%s' 'steps' should be string or JSON data",
				m_name.c_str());
		delete result;
		return false;
	}

	// Load ACL
	ResultSet::ColumnValue *scriptAcl;
	try
	{
		scriptAcl = (*row)->getColumn("acl");
	}
	catch(ResultNoSuchColumnException *e)
	{
		log->error("Script '%s' does not have 'acl' column",
			m_name.c_str());
		delete e;
		delete result;
		return false;
	}
	catch(...)
	{
		log->error("Script '%s': found generic exception while fetching 'acl' column",
			m_name.c_str());
		delete result;
		return false;
	}

	if (!validateACL(service, scriptAcl))
	{
		delete result;
		return false;
	}
	
	// We can continue parsing script steps
	if (doc->IsArray())
	{
		for (auto& item : doc->GetArray())
		{
			if (item.IsObject())
			{
				for (auto& itemValue : item.GetObject())
				{
					string type = itemValue.name.GetString();
					const Value& step = itemValue.value;
					if (step.IsObject())
					{
						int order = 0;
						if (step.HasMember("order") && step["order"].IsInt64())
						{
							order = step["order"].GetInt64();
						}
						else
						{
							log->error("Control script '%s' is badly formatted, %s step is missing an order item",
								m_name.c_str(), type.c_str());
							delete result;
							return false;
						}
						ScriptStep *s = parseStep(type, step);
						if (s != NULL)
						{
							// Pass caller information
							s->setSourceName(m_source_name);
							s->setSourceType(m_source_type);
							s->setRequestURL(m_request_url);

							if (!addStep(order, s))
							{
								log->error("Control script '%s' has more than one step with order of %d", m_name.c_str(), order);
								delete result;
								return false;
							}
						}
						else
						{
							log->error("Control script '%s' is badly formatted, %s script step failed to parse",
								m_name.c_str(), type.c_str());
							delete result;
							return false;
						}
					}
					else
					{
						log->error("Control script '%s' is badly formatted, %s step is not an object",
							m_name.c_str(), type.c_str());
						delete result;
						return false;
					}
				}
			}
			else
			{
				log->error("Control script '%s' is badly formatted, step items should be objects",
						m_name.c_str());
				delete result;
				return false;

			}
		}
	}
	else
	{
		log->error("Control script '%s' is badly formatted, 'steps' should be an array",
				m_name.c_str());
		delete result;
		return false;
	}
	delete result;
	
	m_loaded = true;
	return true;
}

/**
 * Add a step into the script
 *
 * @param stepNo  The step number
 * @param step	  The step to add
 * @return bool True if the step was added
 */
bool Script::addStep(int stepNo, ScriptStep *step)
{
	try {
		ScriptStep *current = m_steps[stepNo];
		if (current)
			return false;
		else
			m_steps[stepNo] = step;
	} catch (...) {
		m_steps.insert(pair<int, ScriptStep *>(stepNo, step));
	}
	return true;
}

/**
 * Parse an individual step in the automation script
 *
 * @param type	The type of the step to parse
 * @param step	The RapidJson Value of the step object
 * @return ScriptStep* The newly created step or NULL on failure
 */
ScriptStep *Script::parseStep(const string& type, const Value& step)
{
	if (type.compare("write") == 0)
	{
		string service;
		KVList values;
		if (step.HasMember("service") && step["service"].IsString())
		{
			service = step["service"].GetString();
		}
		else
		{
			Logger::getLogger()->error("Error parsing step in script '%s', write type steps must contain a service name",
				       	m_name.c_str(), type.c_str());
			return NULL;
		}
		if (step.HasMember("values") && step["values"].IsObject())
		{
			for (auto& v : step["values"].GetObject())
			{
				string name, value;
				name = v.name.GetString();
				value = v.value.GetString();
				values.add(name, value);
			}
		}
		else
		{
			Logger::getLogger()->error("Error parsing step in script '%s', write type steps must contain values",
				       	m_name.c_str(), type.c_str());
			return NULL;
		}
		return new WriteScriptStep(service, values);
	}
	else if (type.compare("operation") == 0)
	{
		string service, operation;
		KVList parameters;
		if (step.HasMember("operation") && step["operation"].IsString())
		{
			operation = step["operation"].GetString();
		}
		else
		{
			Logger::getLogger()->error("Error parsing step in script '%s', operation type steps must contain a operation name",
				       	m_name.c_str(), type.c_str());
			return NULL;
		}
		if (step.HasMember("service") && step["service"].IsString())
		{
			service = step["service"].GetString();
		}
		else
		{
			Logger::getLogger()->error("Error parsing step in script '%s', operation type steps must contain a service name",
				       	m_name.c_str(), type.c_str());
			return NULL;
		}
		if (step.HasMember("parameters") && step["parameters"].IsObject())
		{
			for (auto& v : step["parameters"].GetObject())
			{
				string name, value;
				name = v.name.GetString();
				value = v.value.GetString();
				parameters.add(name, value);
			}
		}
		return new OperationScriptStep(operation, service, parameters);
	}
	else if (type.compare("delay") == 0)
	{
		unsigned int delay;
		if (step.HasMember("duration") && step["duration"].IsInt64())
		{
			delay = step["duration"].GetInt64();
		}
		else
		{
			Logger::getLogger()->error("Error parsing step in script '%s', delay type steps must contain a delay value",
				       	m_name.c_str(), type.c_str());
			return NULL;
		}
		return new DelayScriptStep(delay);
	}
	else if (type.compare("config") == 0)
	{
		string category, name, value;
		if (step.HasMember("category") && step["category"].IsString())
		{
			category = step["category"].GetString();
		}
		else
		{
			Logger::getLogger()->error("Error parsing step in script '%s', config type steps must contain a category name",
				       	m_name.c_str(), type.c_str());
			return NULL;
		}
		if (step.HasMember("name") && step["name"].IsString())
		{
			name = step["name"].GetString();
		}
		else
		{
			Logger::getLogger()->error("Error parsing step in script '%s', config type steps must contain a item name",
				       	m_name.c_str(), type.c_str());
			return NULL;
		}
		if (step.HasMember("value") && step["value"].IsString())
		{
			value = step["value"].GetString();
		}
		else
		{
			Logger::getLogger()->error("Error parsing step in script '%s', config type steps must contain an item value",
				       	m_name.c_str(), type.c_str());
			return NULL;
		}
		return new ConfigScriptStep(category, name, value);
	}
	else if (type.compare("script") == 0)
	{
		string name;
		if (step.HasMember("name") && step["name"].IsString())
		{
			name = step["name"].GetString();
		}
		else
		{
			Logger::getLogger()->error("Error parsing step in script '%s', script type steps must contain a script name",
				       	m_name.c_str(), type.c_str());
			return NULL;
		}
		return new ScriptScriptStep(name);
	}
	else
	{
		Logger::getLogger()->error("Control script '%s' is badly formatted, %s is not a supported script step",
			m_name.c_str(), type.c_str());
		return NULL;
	}
}

/**
 * Add a condition to a script step
 *
 * @param step		The step to add the condition to
 * @param value		The RapidJson value
 * @return bool		True if no errors encountered
 */
bool Script::addCondition(ScriptStep *step, const Value& value)
{
	if (value.HasMember("condition"))
	{
		if (!value["condition"].IsObject())
		{
			Logger::getLogger()->error("Control script '%s', incorrect condition formatting. The condition should be an object", m_name.c_str());
			return false;
		}
		const Value& condition = value["condition"];
		string key, val, op;
		if (condition.HasMember("key") && condition["key"].IsString())
			key = condition["key"].GetString();
		if (condition.HasMember("condition") && condition["condition"].IsString())
			op = condition["condition"].GetString();
		if (condition.HasMember("value") && condition["value"].IsString())
			val = condition["value"].GetString();

		if (key.empty() || val.empty() || op.empty())
		{
			Logger::getLogger()->error("Control script '%s', incorrect condition formatting. The condition object must have a key, condition and value property", m_name.c_str());
			return false;
		}
		step->addCondition(key, op, val);
	}
	return true;	// Unconditional step
}


/**
 * Evalaute a script step condition to see if the step
 * should be executed.
 *
 * @param parameters	The parameters passed to the script
 * @return bool		True if the step should be executed
 */
bool ScriptStep::evaluate(const KVList& parameters)
{
	if (m_key.empty())	// No condition, always execute
	{
		return true;
	}
	string value = parameters.getValue(m_key);
	if (value.empty())
	{
		Logger::getLogger()->warn("The key '%s' was not present in the parameters to the script");
		return false;
	}
	if (m_op.compare("=="))
		return value.compare(m_value) == 0;
	else if (m_op.compare("!="))
		return value.compare(m_value) == 0;
	// TODO add other operations
	return true;
}

/**
 * Execute a WriteScriptStep script step
 *
 * @param service	The Dispatcher service
 * @param parameters	The parameters of the script
 * @return bool		True if the step executed without error
 */
bool WriteScriptStep::execute(DispatcherService *service, const KVList& parameters)
{
	if (!evaluate(parameters))	// The condition evalauted to false, skip the step
	{
		return true;
	}

	m_values.substitute(parameters);

	string payload = "{ \"values\" : ";
	payload += m_values.toJSON();
	payload += " }";

	// Pass m_source_name & m_source_type to south service
	return service->sendToService(m_service,
				"/fledge/south/setpoint",
				payload,
				m_source_name,
				m_source_type);
}

/**
 * Execute a OperationScriptStep script step
 *
 * @param service	The Dispatcher service
 * @param parameters	The parameters of the script
 * @return bool		True if the step executed without error
 */
bool OperationScriptStep::execute(DispatcherService *service, const KVList& parameters)
{
	if (!evaluate(parameters))	// The condition evalauted to false, skip the step
	{
		return true;
	}


	string payload = "{ \"operation\" : \"";
	payload += m_operation;
	payload += "\", ";
	if (m_parameters.size() > 0)
	{
		m_parameters.substitute(parameters);
		payload += "\"parameters\" : ";
		payload += m_parameters.toJSON();
	}
	payload += " }";

	// Pass m_source_name & m_source_type to south service
	return service->sendToService(m_service,
				"/fledge/south/operation",
				payload,
				m_source_name,
				m_source_type);
}

/**
 * Execute a ScriptScriptStep script step
 *
 * @param service	The Dispatcher service
 * @param parameters	The parameters of the script
 * @return bool		True if the step executed without error
 */
bool ScriptScriptStep::execute(DispatcherService *service, const KVList& parameters)
{
	if (!evaluate(parameters))	// The condition evaluated to false, skip the step
	{
		return true;
	}

	// TODO Execution of scripts on the background
	Script script(m_name);

	// Set m_source_name, m_source_name and m_request_url in the Script object
	script.setSourceName(m_source_name);
	script.setSourceType(m_source_type);
	script.setRequestURL(m_request_url);

	return script.execute(service, parameters);
}

/**
 * Execute a ConfigScriptStep script step
 *
 * @param service	The Dispatcher service
 * @param parameters	The parameters of the script
 * @return bool		True if the step executed without error
 */
bool ConfigScriptStep::execute(DispatcherService *service, const KVList& parameters)
{
	if (!evaluate(parameters))	// The condition evalauted to false, skip the step
	{
		return true;
	}
	service->getMgmtClient()->setCategoryItemValue(m_category, m_name, m_value);
}

/**
 * Load ACL for the script and match against:
 * m_source_name, m_source_type and m_request_url
 *
 * @param service	The service object
 * @param scriptAcl	The Database ColumnValue for ACL
 * @return		True is ACL exists and amched
 *			True if ACL does not exist
 *			False if ACL exists and no match
 */
bool Script::validateACL(DispatcherService *service,
			ResultSet::ColumnValue *scriptAcl)
{
	Logger *log = Logger::getLogger();
	StorageClient *storage = service->getStorageClient();

	// Check scriptAcl is a STRING_COLUMN
	if (scriptAcl->getType() == ColumnType::STRING_COLUMN)
	{
		char *str = scriptAcl->getString();
		if (strcmp(str, "") == 0)
		{
			log->debug("Script '%s' has no ACL set",
				m_name.c_str());
			// No ACL to load
			return true;
		}

		log->debug("Script '%s' has ACL '%s', loading it",
			m_name.c_str(),
			str);

		// Need to load the ACL
		Where *where = new Where("name", Equals, str);
		Query scriptQuery(where);
		ResultSet *result = storage->queryTable(ACL_TABLE, scriptQuery);
		if (!result || result->rowCount() != 1)
		{
			log->error("Unable to retrieve a control acl '%s' for script '%s",
				str,
				m_name.c_str());
			if (result)
			{
				delete result;
			}
			return false;
		}

		// TODO add try/catch
		ResultSet::RowIterator row = result->firstRow();
		ResultSet::ColumnValue *serviceCol = (*row)->getColumn("service");

		if (serviceCol->getType() != ColumnType::JSON_COLUMN)
		{
			log->error("Script '%s' ACL '%s': 'service' item is not a JSON data type");
			if (result)
			{
				delete result;
			}
			return false;
		}

		// Check services, by name or type: set matched bool var
		const rapidjson::Value *docS = serviceCol->getJSON();

		if (!docS->IsArray())
		{
			log->error("Script '%s' ACL '%s': 'service' item is not an array");
			if (result)
			{
				delete result;
			}
			return false;
		}

		bool matchedServiceName = false;
		bool matchedServiceType = false;

		// Empty array: allow all
		if (docS->GetArray().Size() == 0)
		{
			matchedServiceName = true;
			matchedServiceType = true;
		}

		// Iterate 'service' array
		for (auto& item : docS->GetArray())
		{
			if (item.IsObject())
			{
				for (auto& itemValue : item.GetObject())
				{
					string key = itemValue.name.GetString();
					string value = itemValue.value.GetString();

					// Match service name
					matchedServiceName = (key == "name") && (value == m_source_name);
					if (matchedServiceName)
					{
						break;
					}
					// Math service type
					matchedServiceType = (key == "type") && (value == m_source_type);
					if (matchedServiceType)
					{
						break;
					}
				}
			}
		}

		if (!matchedServiceType && !matchedServiceName)
		{
			log->error("Execution not allowed to script '%s' "
				"for caller service '%s', type '%s'",
				m_name.c_str(),
				m_source_name.c_str(),
				m_source_type.c_str());
			delete result;
			return false;
		}

		// TODO add try/catch
		// Check URL with value and acl by service type
		ResultSet::ColumnValue *urlCol = (*row)->getColumn("url");
		if (urlCol->getType() != ColumnType::JSON_COLUMN)
		{
			log->error("Script '%s' ACL '%s': 'url' item is not a JSON data type");
			if (result)
			{
				delete result;
			}
			return false;
		}

		const rapidjson::Value *docU = urlCol->getJSON();
		if (!docU->IsArray())
		{
			log->error("Script '%s' ACL '%s': 'service' item is not an array");
			if (result)
			{
				delete result;
			}
			return false;
		}

		matchedServiceType = false;
		bool matchedUrl = false;

		// Empty array: allow all
		if (docU->GetArray().Size() == 0)
		{
			matchedServiceType = true;
			matchedUrl = true;
		}
		// Iterate 'service' array
		for (auto& item : docU->GetArray())
		{
			if (item.IsObject())
			{
				string url;
				for (auto& itemValue : item.GetObject())
				{
					string key = itemValue.name.GetString();
					if (key == "url")
					{
						url = itemValue.value.GetString();
						if (url != "")
						{
							matchedUrl = (url == m_request_url);
						}
					}
					if (key == "acl")
					{
						if (itemValue.value.IsArray())
						{
							// Empty ACL array for service type: matchedServiceType is true
							matchedServiceType = (itemValue.value.GetArray().Size() == 0);

							for (auto& a : itemValue.value.GetArray())
							{
								if (a.IsObject())
								{
									for (auto& iv : a.GetObject())
									{
										string key = iv.name.GetString();
										string value = iv.value.GetString();
										// Math service type
										matchedServiceType = (key == "type") &&
												(value == m_source_type);
										if (matchedServiceType)
										{
											break;
										}
									}
								}
							}
						}
					}

					if (matchedUrl || matchedServiceType)
					{
						break;
					}
				}
			}
		}

		if (!matchedServiceType && !matchedUrl)
		{
			log->error("Execution not allowed to script '%s' "
				" for caller URL '%s', service name '%s', type '%s'",
				m_name.c_str(),
				m_source_name.c_str(),
				m_source_type.c_str(),
				m_request_url.c_str());
			delete result;
			return false;
		}

		// Remove fetched dataset
		delete result;
	}
	else
	{
		log->error("Loading script '%s', ACL item is not a string data type",
			m_name.c_str());
		return false;
	}

	return true;
}
