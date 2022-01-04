/*
 * Fledge Dispatcher API class for dispatcher control request classes
 *
 * Copyright (c) 2021 Dianomic Systems
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Mark Riddoch
 */
#include <automation.h>
#include <dispatcher_service.h>
#include <query.h>
#include <rapidjson/document.h>

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
	for (auto it = m_steps.begin(); it != m_steps.end(); ++it)
	{
		bool res = it->second->execute(service, parameters);
		if (!res)
		{
			return res;
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
	StorageClient *storage = service->getStorageClient();

	Where where("name", Equals, m_name);
	Query scriptQuery(&where);
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
	ResultSet::ColumnValue *scriptCol = (*row)->getColumn("steps");
	const Value *json = scriptCol->getJSON();
	if (json->HasMember("steps"))
	{
		const Value& steps = (*json)["steps"];
		if (steps.IsArray())
		{
			for (auto& item : steps.GetArray())
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
								addStep(order, s);
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
			log->error("Control script '%s' is badly formatted, steps should be an array",
					m_name.c_str());
			delete result;
			return false;
		}
	}
	else
	{
		log->error("Control script '%s' is missing steps", m_name.c_str());
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
 * @param srep	  The step to add
 * @return bool True if the step was added
 */
bool Script::addStep(int stepNo, ScriptStep *step)
{
	try {
		ScriptStep *current = m_steps[stepNo];
		return false;
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
		if (step.HasMember("delay") && step["delay"].IsInt64())
		{
			delay = step["delay"].GetInt64();
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

	m_values.substitute(m_values);

	string payload = "{ \"values\" : { ";
	payload += m_values.toJSON();
	payload += "\" } }";
	return service->sendToService(m_service, "/fledge/south/setpoint", payload);
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
		payload += "\"parameters\" : { ";
		payload += m_parameters.toJSON();
		payload += "} ";
	}
	payload += " }";
	return service->sendToService(m_service, "/fledge/south/operation", payload);
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
	// TODO add the code to execute the configuration change
}

