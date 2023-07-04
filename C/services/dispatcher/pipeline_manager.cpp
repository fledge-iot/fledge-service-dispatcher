/*
 * Fledge Dispatcher service.
 *
 * Copyright (c) 2023 Dianomic Systems
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Mark Riddoch
 *
 */

#include <pipeline_manager.h>
#include <controlpipeline.h>
#include <dispatcher_service.h>

using namespace std;

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
					// TODO add enabled and execution fields
					vector<string> filters;
					loadFilters(pname, cpid->getInteger(), filters);
					pipe->setPipeline(filters);
					m_pipelines[pname] = pipe;
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

	m_logger->debug("%d pipelines have benn loaded", m_pipelines.size());
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
		ResultSet::RowIterator it = results->firstRow();
		do {
			ResultSet::Row *row = *it;
			if (row)
			{
				ResultSet::ColumnValue *name = row->getColumn("fname");
				filters.emplace_back(name->getString());
			}
		} while (results->isLastRow(it++));
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
 * to reconfigure the filters inthe various pipelines when a category is changed.
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
 * A configuration category has changed. Fidn all the filter plugins that have registered
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
	}
}