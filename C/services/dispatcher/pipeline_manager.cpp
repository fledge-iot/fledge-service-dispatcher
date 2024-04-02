/*
 * Fledge Dispatcher service.
 *
 * Copyright (c) 2023 Dianomic Systems
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Mark Riddoch, Massimiliano Pinto
 *
 */

#include <pipeline_manager.h>
#include <controlpipeline.h>
#include <dispatcher_service.h>
#include <rapidjson/document.h>

using namespace std;
using namespace rapidjson;

/**
 * Constructor for the Control Pipelien Manager
 *
 * @param storage	The storage client used communicate with the soteage service
 */
ControlPipelineManager::ControlPipelineManager(ManagementClient *mgtClient, StorageClient *storage) : 
	m_managementClient(mgtClient), m_storage(storage)
{
	m_logger = Logger::getLogger();
}

/**
 * Destructor for the Control Pipeline Manager
 */
ControlPipelineManager::~ControlPipelineManager()
{
}

/**
 * Do the intial load of the control pipelines at startup.
 * Changes to the pipelines are subseqently collected by
 * monitoring inserts and updates to the database tables.
 */
void
ControlPipelineManager::loadPipelines()
{
	lock_guard<mutex> guard(m_pipelinesMtx);	// Prevent use of pipelines
	loadLookupTables();
	vector<Returns *>columns;
	columns.push_back(new Returns ("cpid"));
	columns.push_back(new Returns ("name"));
	columns.push_back(new Returns ("stype"));
	columns.push_back(new Returns ("sname"));
	columns.push_back(new Returns ("dtype"));
	columns.push_back(new Returns ("dname"));
	columns.push_back(new Returns ("enabled"));
	columns.push_back(new Returns ("execution"));
	Query allPipelines(columns);
	try {
		ResultSet *pipelines = m_storage->queryTable(PIPELINES_TABLE, allPipelines);
		if (pipelines->rowCount() > 0)
		{
			ResultSet::RowIterator it = pipelines->firstRow();
			do
			{
				ResultSet::Row *row = *it;
				if (row)
				{
					ResultSet::ColumnValue *cpid = row->getColumn("cpid");
					long pipelineId = cpid->getInteger();
					ResultSet::ColumnValue *name = row->getColumn("name");
					string pname = name->getString();

					ControlPipeline *pipe = new ControlPipeline(this, name->getString());
					if (!pipe)
					{
						m_logger->error("Failed to allocate the '%s' control pipeline",
								pname.c_str());
						break;
					}
					ResultSet::ColumnValue *cpsid = row->getColumn("stype");
					PipelineEndpoint::EndpointType stype = m_sourceTypes[cpsid->getInteger()].m_type;
					ResultSet::ColumnValue *sname = row->getColumn("sname");
					PipelineEndpoint source(stype, sname->getString());
					ResultSet::ColumnValue *cpdid = row->getColumn("dtype");
					PipelineEndpoint::EndpointType dtype = m_destTypes[cpdid->getInteger()].m_type;
					ResultSet::ColumnValue *dname = row->getColumn("dname");
					PipelineEndpoint dest(dtype, dname->getString());
					pipe->endpoints(source, dest);
					vector<string> filters;
					loadFilters(pname, cpid->getInteger(), filters);
					pipe->setPipeline(filters);
					ResultSet::ColumnValue *en = row->getColumn("enabled");
					pipe->enable(en->getString()[0] == 't');
					m_pipelines[pname] = pipe;
					m_pipelineIds[pipelineId] = pname;
				}
				if (! pipelines->isLastRow(it))
					it++;
			} while (! pipelines->isLastRow(it));
		}
		delete pipelines;
	} catch (exception* exp) {
		m_logger->error("Exception loading control pipelines: %s", exp->what());
	} catch (exception& ex) {
		m_logger->error("Exception loading control pipelines: %s", ex.what());
	} catch (...) {
		m_logger->error("Exception loading control pipelines");
	}

	m_logger->debug("%d pipelines have been loaded", m_pipelines.size());

	// Register for updates to the table
	m_dispatcher->registerTable(PIPELINES_TABLE);
	m_dispatcher->registerTable(PIPELINES_FILTER_TABLE);
	return;
}

/**
 * Populate the filter names for the given pipeline
 *
 * @param pipeline	The name of the pipeline
 * @param cpid		Control pipeline ID
 * @param filters	The array to populate with the filter names
 */
void ControlPipelineManager::loadFilters(const string& pipeline, int cpid, vector<string>& filters)
{
	vector<Returns *>columns;
	columns.push_back(new Returns ("fname"));
	Where *where = new Where("cpid", Equals, to_string(cpid));
	Sort *orderby = new Sort("forder");
	Query allFilters(columns, where);
	allFilters.sort(orderby);
	try {
		ResultSet *results = m_storage->queryTable(PIPELINES_FILTER_TABLE, allFilters);
		if (results->rowCount() > 0) {
			ResultSet::RowIterator it = results->firstRow();
			do {
				ResultSet::Row *row = *it;
				if (row)
				{
					ResultSet::ColumnValue *name = row->getColumn("fname");
					filters.emplace_back(name->getString());
				}
			} while (! results->isLastRow(it++));
		}
		delete results;
	} catch (exception* exp) {
		m_logger->error("Exception loading control pipeline filters for pipeline %s: %s", pipeline.c_str(), exp->what());
	} catch (exception& ex) {
		m_logger->error("Exception loading control pipeline filters for pipeline %s: %s", pipeline.c_str(), ex.what());
	} catch (...) {
		m_logger->error("Exception loading control pipeline filters for pipeline %s", pipeline.c_str());
	}
}

/**
 * Find the control pipeline that best matches the source
 * and destination end points given.
 *
 * @param source	The source end point to find a control pipeline for
 * @param dest		The destination end point to find a control pipeline for
 * @return ControlPipeline*	The best match control pipelien or NULL if none matched
 */
ControlPipeline *
ControlPipelineManager::findPipeline(const PipelineEndpoint& source, const PipelineEndpoint& dest)
{
	lock_guard<mutex> guard(m_pipelinesMtx);
	// First of all look for a pipeline that exactly matches our source and destination
	for (auto const& pipe : m_pipelines)
	{
		if (pipe.second->match(source, dest))
		{
			m_logger->debug("%s exactly matches pipelines for source %s to destination %s",
				pipe.second->getName().c_str(), source.toString().c_str(), dest.toString().c_str());
			return pipe.second;
		}
	}
	PipelineEndpoint any(PipelineEndpoint::EndpointAny);

	// Look for a pipeline that matches our destination and any source
	for (auto const& pipe : m_pipelines)
	{
		if (pipe.second->match(any, dest))
		{
			m_logger->debug("%s matches pipelines for source (ANY) %s to destination %s",
				pipe.second->getName().c_str(), source.toString().c_str(), dest.toString().c_str());
			return pipe.second;
		}
	}

	// Look for a pipeline that matches our source and any destination
	for (auto const& pipe : m_pipelines)
	{
		if (pipe.second->match(source, any))
		{
			m_logger->debug("%s matches pipelines for source %s to destination (ANY) %s",
				pipe.second->getName().c_str(), source.toString().c_str(), dest.toString().c_str());
			return pipe.second;
		}
	}

	// Finally look for any source and any destination
	for (auto const& pipe : m_pipelines)
	{
		if (pipe.second->match(any, any))
		{
			m_logger->debug("%s matches pipelines for source %s to destination %s with generic any to any pipeline",
				pipe.second->getName().c_str(), source.toString().c_str(), dest.toString().c_str());
			return pipe.second;
		}
	}

	// No pipelines matched
	m_logger->debug("No matching pipelines for source %s to destination %s",
			source.toString().c_str(), dest.toString().c_str());
	return NULL;
}

/**
 * Load the lookup tables for the endpoint types
 */
void ControlPipelineManager::loadLookupTables()
{
	// First load the source endpoints
	vector<Returns *>columns;
	columns.push_back(new Returns ("cpsid"));
	columns.push_back(new Returns ("name"));
	columns.push_back(new Returns ("description"));
	Query allSource(columns);
	try {
		ResultSet *sources = m_storage->queryTable(SOURCES_TABLE, allSource);
		ResultSet::RowIterator it = sources->firstRow();
		do {
			ResultSet::Row *row = *it;
			if (row)
			{
				ResultSet::ColumnValue *cpsid = row->getColumn("cpsid");
				ResultSet::ColumnValue *name = row->getColumn("name");
				ResultSet::ColumnValue *description = row->getColumn("description");
				
				PipelineEndpoint::EndpointType t = PipelineEndpoint::EndpointAny;
				if (!strcmp(name->getString(),"Any"))
					t = PipelineEndpoint::EndpointAny;
				else if (!strcmp(name->getString(),"Service"))
					t = PipelineEndpoint::EndpointService;
				else if (!strcmp(name->getString(),"API"))
					t = PipelineEndpoint::EndpointAPI;
				else if (!strcmp(name->getString(),"Notification"))
					t = PipelineEndpoint::EndpointNotification;
				else if (!strcmp(name->getString(),"Schedule"))
					t = PipelineEndpoint::EndpointSchedule;
				else if (!strcmp(name->getString(),"Script"))
					t = PipelineEndpoint::EndpointScript;
				EndpointLookup epl(name->getString(), description->getString(), t);
				m_sourceTypes[cpsid->getInteger()] = epl;
			}
		} while (sources->isLastRow(it++));
		delete sources;
	} catch (exception* exp) {
		m_logger->error("Exception loading control pipeline sources: %s", exp->what());
	} catch (exception& ex) {
		m_logger->error("Exception loading control pipeline sources: %s", ex.what());
	} catch (...) {
		m_logger->error("Exception loading control pipeline sources");
	}

	// Now  load the destination endpoints
	columns.clear();
	columns.push_back(new Returns ("cpdid"));
	columns.push_back(new Returns ("name"));
	columns.push_back(new Returns ("description"));
	Query allDest(columns);
	try {
		ResultSet *dests = m_storage->queryTable(DESTINATIONS_TABLE, allDest);
		if (dests)
		{
			ResultSet::RowIterator it = dests->firstRow();
			do {
				ResultSet::Row *row = *it;
				if (row)
				{
					ResultSet::ColumnValue *cpdid = row->getColumn("cpdid");
					ResultSet::ColumnValue *name = row->getColumn("name");
					ResultSet::ColumnValue *description = row->getColumn("description");
					
					PipelineEndpoint::EndpointType t = PipelineEndpoint::EndpointAny;
					if (!strcmp(name->getString(),"Asset"))
						t = PipelineEndpoint::EndpointAsset;
					else if (!strcmp(name->getString(),"Service"))
						t = PipelineEndpoint::EndpointService;
					else if (!strcmp(name->getString(),"Broadcast"))
						t = PipelineEndpoint::EndpointBroadcast;
					else if (!strcmp(name->getString(),"Script"))
						t = PipelineEndpoint::EndpointScript;
					EndpointLookup epl(name->getString(), description->getString(), t);
					m_destTypes[cpdid->getInteger()] = epl;
				}
			} while (dests->isLastRow(it++));
			delete dests;
		}
	} catch (exception* exp) {
		m_logger->error("Exception loading control pipeline destinations: %s", exp->what());
	} catch (exception& ex) {
		m_logger->error("Exception loading control pipeline destinations: %s", ex.what());
	} catch (...) {
		m_logger->error("Exception loading control pipeline destinations");
	}
}

/**
 * Find the end point type given the string version of the type name
 *
 * @param typeName	String type of end point type
 * @param source	Use source end point types
 * @return PipelineEndpoint::EndpointType  The endpoint type, returns EndpointUndefined if there is no match
 */
PipelineEndpoint::EndpointType
ControlPipelineManager::findType(const string& typeName, bool source)
{
	PipelineEndpoint::EndpointType rval = PipelineEndpoint::EndpointUndefined;

	if (source)
	{
		for (auto const &lkup : m_sourceTypes)
		{
			if (lkup.second.m_name.compare(typeName) == 0)
				rval = lkup.second.m_type;
		}
	}
	else
	{
		for (auto const &lkup : m_destTypes)
		{
			if (lkup.second.m_name.compare(typeName) == 0)
				rval = lkup.second.m_type;
		}
	}
	return rval;
}

/**
 * Constructor for EndpointLookup class
 */
ControlPipelineManager::EndpointLookup::EndpointLookup(const std::string& name,
		const std::string& description, PipelineEndpoint::EndpointType type) :
			m_name(name), m_description(description), m_type(type)
{
}

/**
 * Copy constructor for EndpointLookup class
 */
ControlPipelineManager::EndpointLookup::EndpointLookup(const ControlPipelineManager::EndpointLookup& rhs)
{
	m_type = rhs.m_type;
	m_name = rhs.m_name;
	m_description = rhs.m_description;
}

/**
 * Register a category name for a filter plugin. This allows the pipeline manager
 * to reconfigure the filters in the various pipelines when a category is changed.
 *
 * @param category	The name of the category to register
 * @param plugin	The plugin that requires the category
 */
void ControlPipelineManager::registerCategory(const string& category, FilterPlugin *plugin)
{
	m_categories.insert(pair<string, FilterPlugin *>(category, plugin));
	m_dispatcher->registerCategory(category);
}

/**
 * Unregister a category name for a filter plugin. This removes the registration
 * in order to prevent the plugin's reconfigure method being called when the category
 * is changed. This should be called whenever a plugin is deleted.
 *
 * @param category	The name of the category to register
 * @param plugin	The plugin that requires the category
 */
void ControlPipelineManager::unregisterCategory(const string& category, FilterPlugin *plugin)
{
	auto it = m_categories.find(category);
	while (it != m_categories.end())
	{
		if (it->second == plugin)
		{
			m_categories.erase(it);
			return;
		}
		it++;
	}
}

/**
 * A configuration category has changed. Find all the filter plugins that have registered
 * an interest in the category and call the reconfigure method of the plugin.
 *
 * @param name		The name of the configuration category
 * @param content	The content of the configuration category
 */
void ControlPipelineManager::categoryChanged(const string& name, const string& content)
{
	auto it = m_categories.find(name);
	while (it != m_categories.end())
	{
		it->second->reconfigure(content);
		it++;
	}
}

/**
 * Called whenever we get a row insert event from the storage service for
 * one of the tables we are monitoring.
 *
 * @param table	The name of the table that the insert occurred on
 * @param doc	Rapidjson document with the row contents
 */
void ControlPipelineManager::rowInsert(const string& table, const Document& doc)
{
	if (table.compare(PIPELINES_TABLE) == 0)
	{
		insertPipeline(doc);
	}
	else if (table.compare(PIPELINES_FILTER_TABLE) == 0)
	{
		insertPipelineFilter(doc);
	}
}

/**
 * Called whenever we get a row update event from the storage service for
 * one of the tables we are monitoring.
 *
 * @param table	The name of the table that the update occurred on
 * @param doc	Rapidjson document with the row contents
 */
void ControlPipelineManager::rowUpdate(const string& table, const Document& doc)
{
	if (table.compare(PIPELINES_TABLE) == 0)
	{
		updatePipeline(doc);
	}
	else if (table.compare(PIPELINES_FILTER_TABLE) == 0)
	{
		updatePipelineFilter(doc);
	}
}

/**
 * Called whenever we get a row delete event from the storage service for
 * one of the tables we are monitoring.
 *
 * @param table	The name of the table that the delete occurred on
 * @param doc	Rapidjson document with the row contents
 */
void ControlPipelineManager::rowDelete(const string& table, const Document& doc)
{
	if (table.compare(PIPELINES_TABLE) == 0)
	{
		deletePipeline(doc);
	}
	else if (table.compare(PIPELINES_FILTER_TABLE) == 0)
	{
		deletePipelineFilter(doc);
	}
}

/**
 * Pipeline insert - handle an update to the pipelines table. This will be passed a
 * JSON document with the new row in it.
 *
 * The document passed will look as follows
 * {"name": "test3", "enabled": "t", "execution": "Exclusive", "stype": 2, "sname": "OpenOPCUA", "dtype": 4, "dname": ""}
 *
 * @param doc	The new pipeline table contents
 */
void ControlPipelineManager::insertPipeline(const Document& doc)
{
	// Check all ther fields values are present and correct
	if (doc.HasMember("name") == false || doc["name"].IsString() == false)
	{
		return;
	}
	if (doc.HasMember("enabled") == false || doc["enabled"].IsString() == false)
	{
		return;
	}
	if (doc.HasMember("execution") == false || doc["execution"].IsString() == false)
	{
		return;
	}
	if (doc.HasMember("sname") == false || doc["sname"].IsString() == false)
	{
		return;
	}
	if (doc.HasMember("dname") == false || doc["dname"].IsString() == false)
	{
		return;
	}
	if (doc.HasMember("stype") == false || doc["stype"].IsInt() == false)
	{
		return;
	}
	if (doc.HasMember("dtype") == false || doc["dtype"].IsInt() == false)
	{
		return;
	}

	string pname = doc["name"].GetString();
	ControlPipeline *pipe = new ControlPipeline(this, pname);
	if (!pipe)
	{
		m_logger->error("Failed to allocate the '%s' control pipeline",
								pname.c_str());
	}
	else
	{
		PipelineEndpoint::EndpointType stype = m_sourceTypes[doc["stype"].GetInt()].m_type;
		PipelineEndpoint source(stype, doc["sname"].GetString());
		PipelineEndpoint::EndpointType dtype = m_destTypes[doc["dtype"].GetInt()].m_type;
		PipelineEndpoint dest(dtype, doc["dname"].GetString());
		pipe->endpoints(source, dest);
		string en = doc["enabled"].GetString();
		if (en.compare("t") == 0)
		{
			pipe->enable(true);
		}
		else
		{
			pipe->enable(false);
		}
		string ex = doc["execution"].GetString();
		if (ex.compare("Exclusive") == 0)
		{
			pipe->exclusive(true);
		}
		else
		{
			pipe->exclusive(false);
		}

		lock_guard<mutex> guard(m_pipelinesMtx);

		// Load pipeline from storage and get cpid value
		long pipelineId = loadPipeline(pname);
		if (pipelineId > 0)
		{
			// store pipeline object
			m_pipelines[pname] = pipe;

			// store pipeline id
			m_pipelineIds[pipelineId] = pname;
		} else {
			m_logger->error("Failed to setup control pipeline '%s'",
					pname.c_str());
		}
	}
}

/**
 * Called when a new filter is inserted into a pipeline. The document
 * passed contains the database row that was inserted.
 *
 * {"cpid": "3", "forder": 1, "fname": "ctrl_test3_exp1"}
 * @param doc	The new filter table contents
 */
void ControlPipelineManager::insertPipelineFilter(const Document& doc)
{
	int id, order;
	string filter;

	if (doc.HasMember("cpid") && doc["cpid"].IsInt())
	{
		id = doc["cpid"].GetInt();
	}
	else
	{
		return;
	}
	if (doc.HasMember("forder") && doc["forder"].IsInt())
	{
		order = doc["forder"].GetInt();
	}
	else
	{
		return;
	}
	if (doc.HasMember("fname") && doc["fname"].IsString())
	{
		filter = doc["fname"].GetString();
	}
	else
	{
		return;
	}

	// Find the name of the pipeline
	string name = m_pipelineIds[id];
	if (name.empty())
	{
		m_logger->error("Unable to process addition of filter %s to pipeline as unable to find matching pipeline",
				filter.c_str());
		return;
	}

	ControlPipeline *pipeline = m_pipelines[name];
	if (pipeline)
	{
		pipeline->addFilter(filter, order);
	}
	else
	{
		m_logger->error("Unable to process addition of filter %s to pipeline %s as unable to find matching pipeline",
				filter.c_str(), name.c_str());
	}
}

/**
 * Pipeline update - handle an update to the pipelines table. This will be passed a
 * JSON document with the new row in it.
 *
 * The document passed will look as follows
 *
 * {"values": {"enabled": "t", "execution": "Shared", "stype": 1, "sname": "", "dtype": 4, "dname": ""}, "where": {"column": "cpid", "condition": "=", "value": "1"}}

 * @param doc	The new pipeline table contents
 */
void ControlPipelineManager::updatePipeline(const Document& doc)
{
	string value = getFromJSONWhere(doc, "cpid");
	if (value.empty())
	{
		m_logger->error("Unable to determine ID of updated pipeline, ignoring update");
		return;
	}
	lock_guard<mutex> guard(m_pipelinesMtx);
	long cpid = strtol(value.c_str(), NULL, 10);

	string pipelineName = m_pipelineIds[cpid];
	if (pipelineName.empty())
	{
		m_logger->error("Unable to determine name of updated pipeline %d, ignoring update", cpid);
		return;
	}

	ControlPipeline *pipeline = m_pipelines[pipelineName];
	if (doc.HasMember("values") && doc["values"].IsObject())
	{
		const Value& values = doc["values"];
		for (auto& column : values.GetObject())
		{
			string name = column.name.GetString();
			if (name.compare("enabled") == 0 && column.value.IsString())
			{
				string value = column.value.GetString();
				pipeline->enable(value.compare("t") == 0 ? true : false);
			}
			else if (name.compare("execution") == 0 && column.value.IsString())
			{
				string value = column.value.GetString();
				pipeline->exclusive(value.compare("Shared") == 0 ? false : true);
			}
			// TODO action the endpoint changes
		}
	}
}

/**
 * Called when a new filter is inserted into a pipeline. The document
 * passed contains the database row that was inserted.
 *
 * {"values": {"forder": 2}, "where": {"column": "fname", "condition": "=", "value": "ctrl_test1_rename", "and": {"column": "cpid", "condition": "=", "value": "1"}}}
 * @param doc	The new filter table contents
 */
void ControlPipelineManager::updatePipelineFilter(const Document& doc)
{
	string value = getFromJSONWhere(doc, "cpid");
	if (value.empty())
	{
		m_logger->error("Unable to determine ID of updated pipeline, ignoring update");
		return;
	}
	long cpid = strtol(value.c_str(), NULL, 10);
	string filter = getFromJSONWhere(doc, "fname");
	if (filter.empty())
	{
		m_logger->error("Unable to determine the name of the filter to reorder");
		return;
	}

	auto pipelineIdIterator = m_pipelineIds.find(cpid);
	if (pipelineIdIterator == m_pipelineIds.end()) {
		m_logger->error("Unable to find pipeline with id %d, filter pipeline update ignored", cpid);
		return;
	}
	auto name = pipelineIdIterator->second;
	auto pipelineIterator = m_pipelines.find(name);
	if (pipelineIterator == m_pipelines.end()) {
		m_logger->error("Pipeline %s has not been loaded, update ignored", name.c_str());
		return;
	}

	ControlPipeline *pipeline = pipelineIterator->second;
	// We have the pipeline ID, not work out what has changed
	if (doc.HasMember("values") && doc["values"].IsObject())
	{
		const Value& values = doc["values"];
		for (auto& column : values.GetObject())
		{
			string name = column.name.GetString();

			if (name.compare("forder") == 0)
			{
				if (!column.value.IsInt())
				{
					m_logger->error("The order in the pipeline is expected to be an integer but is not, ignoring reorder");
				}
				else
				{
					int ord = column.value.GetInt();
					// A filter re-order
					pipeline->reorder(filter, ord);
				}
			}
		}

	}
}

/**
 * Pipeline delete - handle a delete to the pipelines table. This will be passed a
 * JSON document with the new row in it.
 *
 * The document passed will look as follows
 * {"where": {"column": "cpid", "condition": "=", "value": 4}}
 *
 * @param doc	The deleted pipeline table contents
 */
void ControlPipelineManager::deletePipeline(const Document& doc)
{
	string value = getFromJSONWhere(doc, "cpid");

	if (value.empty())
	{
		m_logger->error("Unable to determine ID of pipeline to delete, ignoring delete");
		return;
	}
	long cpid = strtol(value.c_str(), NULL, 10);
	string pipelineName = m_pipelineIds[cpid];

	m_pipelineIds.erase(cpid);
	ControlPipeline *pipeline = m_pipelines[pipelineName];
	m_pipelines.erase(pipelineName);
}

/**
 * Called when a new filter is inserted into a pipeline. The document
 * passed contains the database row that was inserted.
 *
 * {"where": {"column": "cpid", "condition": "=", "value": "1", "and": {"column": "fname", "condition": "=", "value": "ctrl_test1_del"}}}
 * @param doc	The new filter table contents
 */
void ControlPipelineManager::deletePipelineFilter(const Document& doc)
{
	string value = getFromJSONWhere(doc, "cpid");
	if (value.empty())
	{
		m_logger->error("Unable to determine ID of updated pipeline, ignoring update");
		return;
	}
	long cpid = strtol(value.c_str(), NULL, 10);

	// Now find the name of the filter to remove
	string filter = getFromJSONWhere(doc, "fname");;
	if (filter.empty())
	{
		m_logger->error("Unable to determine the name of the filter to remove from the pipeline, no filters will be removed");
		return;
	}

	string pipelineName = m_pipelineIds[cpid];
	if (pipelineName.empty())
	{
		m_logger->error("Unable to pipeline %d to remove filter from, ignoring", cpid);
		return;
	}
	ControlPipeline *pipeline = m_pipelines[pipelineName];
	if (!pipeline)
		return;
	pipeline->removeFilter(filter);
}

/**
 * Extract a given ID from a JSON where clause that is part of a database
 * table change notification
 *
 * @param doc	The JSON document containing the where clause
 * @param key	The key to extract
 * @return string	The key value
 */
string ControlPipelineManager::getFromJSONWhere(const Document& doc, const string& key)
{
	string result;
	// Find the key in the where clause
	if (doc.HasMember("where") && doc["where"].IsObject())
	{
		const Value& where = doc["where"];
		if (where.HasMember("column") && where["column"].IsString()
				&& key.compare(where["column"].GetString()) == 0)
		{
			if (where.HasMember("value") && where["value"].IsString())
			{
				result = std::to_string(strtol(where["value"].GetString(), NULL, 10));
			}
			else if (where.HasMember("value") && where["value"].IsInt64())
			{
				result = std::to_string(where["value"].GetInt64());
			}
			else
			{
				if (where.HasMember("value") && where["value"].IsInt())
				{
					result = std::to_string(where["value"].GetInt());
				}
                        }
		}
		else if (where.HasMember("and") && where["and"].IsObject())
		{
			const Value& second = where["and"];
			if (second.HasMember("column") && second["column"].IsString()
					&& key.compare(second["column"].GetString()) == 0)
			{
				if (second.HasMember("value") && second["value"].IsString())
				{
					result = strtol(second["value"].GetString(), NULL, 10);
				}
				else if (second.HasMember("value") && second["value"].IsInt64())
				{
					result = second["value"].GetInt64();
				}
				else
				{
					if (second.HasMember("value") && second["value"].IsInt())
					{
						result = second["value"].GetInt();
					}
				}
			}
		}
	}

	return result;
}

/**
 * Do the load of a given control pipeline name (just added)
 * from storage and get the cpid value and store it in memory
 *
 * @param name The pipeline name to load from storage
 * @return piplineId or -1 in case of errors
 */
long
ControlPipelineManager::loadPipeline(string& pName)
{
	vector<Returns *>columns;
	// Look for pname pipeline in storage
	Where *where = new Where("name", Equals, pName);
	// Get back cpid and name columns
	columns.push_back(new Returns ("cpid"));
	columns.push_back(new Returns ("name"));
	Query aPipeline(columns, where);

	long pipelineId = -1;

	try {
		ResultSet *pipeline = m_storage->queryTable(PIPELINES_TABLE, aPipeline);
		if (pipeline->rowCount() > 0)
		{
			ResultSet::RowIterator it = pipeline->firstRow();
			do
			{
				ResultSet::Row *row = *it;
				if (row)
				{
					ResultSet::ColumnValue *name = row->getColumn("name");
					string pipelineName = name->getString();
					if (pipelineName == pName) {
						// Match found
						ResultSet::ColumnValue *cpid = row->getColumn("cpid");

						// Set return value
						pipelineId = cpid->getInteger();
						break;
					}
				}
				if (! pipeline->isLastRow(it))
				{
					it++;
				}
			}  while (! pipeline->isLastRow(it));
		}
		delete pipeline;
	} catch (exception* exp) {
		m_logger->error("Exception loading control pipeline '%s': %s",
				pName.c_str(),
				exp->what());
	} catch (exception& ex) {
		m_logger->error("Exception loading control pipeline '%s': %s",
				pName.c_str(),
				ex.what());
	} catch (...) {
		m_logger->error("Exception loading control pipeline '%s'",
				pName.c_str());
	}

	return pipelineId;
}
