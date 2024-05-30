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
#include <pipeline_execution.h>
#include <pipeline_manager.h>
#include <plugin_manager.h>
#include <filter_plugin.h>

using namespace std;

/**
 * Constructor for pipeline exeuction context
 *
 * @param management	A management client pointer
 * @param name		The name of the pipeline this execution context is used for
 * @param filters	The list of filters to be run in the pipeline
 */
PipelineExecutionContext::PipelineExecutionContext(ManagementClient *management, const std::string& name, const vector<string>& filters) :
	m_management(management), m_name(name), m_filters(filters)
{
	m_logger = Logger::getLogger();
}

/**
 * Destructor for the Pipeline Execution Context.
 *
 * Shutdown the filters and reclaim any resources
 *
 * Destruction requires that there are no control operations currently using
 * the context. We need to wait until we can grab the mutex before proceeding,
 * however this is not suffcient as if there are any other threads waiting on
 * the mutex they will grab this after we have destroyed the context and hence
 * fail. The layer above us, the pipeline manager takes responbsibility for this.
 */
PipelineExecutionContext::~PipelineExecutionContext()
{
	lock_guard<mutex> guard(m_mutex);

	for (int i = 0; i < m_plugins.size(); i++)
	{
		m_plugins[i]->shutdown();
		delete m_plugins[i];
	}

}

/**
 * Load all the filters in the pipeline and setup ready for execution
 */
bool
PipelineExecutionContext::loadPipeline()
{
bool rval = true;

	m_logger->debug("Load Pipeline %s", m_name.c_str());
	try
	{
		for (auto& categoryName : m_filters)
		{
			// Get the category with values and defaults
			ConfigCategory config = m_management->getCategory(categoryName);
			if (config.itemExists("plugin"))
			{
				string filterName = config.getValue("plugin");
				m_logger->debug("Loading the plugin '%s' for filter %s", filterName.c_str(), categoryName.c_str());
				PLUGIN_HANDLE filterHandle;
				// Load filter plugin to obtain a handle that we can use to call the plugin
				filterHandle = loadFilterPlugin(filterName);
				if (!filterHandle)
				{
					string errMsg("Cannot load filter plugin '" + filterName + "'");
					m_logger->error(errMsg.c_str());
					rval = false;
				}
				else
				{
					PluginManager *pluginManager = PluginManager::getInstance();
					// Get plugin default configuration
					string filterConfig = pluginManager->getInfo(filterHandle)->config;

					// Create/Update default filter category items
					DefaultConfigCategory filterDefConfig(categoryName, filterConfig);
					string filterDescription = "Configuration of '" + filterName;
					filterDescription += "' filter for plugin '" + categoryName + "'";
					filterDefConfig.setDescription(filterDescription);

					if (!m_management->addCategory(filterDefConfig, true))
					{
						string errMsg("Cannot create/update '" + \
							      categoryName + "' filter category");
						m_logger->error(errMsg.c_str());
						rval = false;
					}
					else
					{

						// Instantiate the FilterPlugin class
						// in order to call plugin entry points
						FilterPlugin *currentFilter = new FilterPlugin(filterName,
											       filterHandle);

						// Add filter to filters vector
						m_plugins.push_back(currentFilter);
					}
				}
			}
			if (!rval)
				break;
		}
	}
	catch (ConfigItemNotFound* e)
	{
		delete e;
		m_logger->info("loadFilters: no filters configured for pipeline %s", m_name.c_str());
		return true;
	}
	catch (exception& e)
	{
		m_logger->error("loadFilters: failed to handle exception for pipeline %s, %s",
				m_name.c_str(), e.what());
		return false;
	}
	catch (...)
	{
		m_logger->error("loadFilters: generic exception while loading %s", m_name.c_str());
		return false;
	}

	// The filters in the pipeline are now loaded. Connect them together
	// to form the actual pipeline by calling the init method of the plugin.
	// This is passed the now merged configuration category and the next step
	// in the pipeline.
	int i = 0;
	for (auto it = m_plugins.begin(); it != m_plugins.end(); ++it, ++i)
	{
		FilterPlugin *plugin = *it;
		ConfigCategory updatedCfg = m_management->getCategory(m_filters[i]);
		if ((it + 1) != m_plugins.end())
		{
			plugin->init(updatedCfg, *(it + 1), filterReadingSetFn(passToOnwardFilter));
		}
		else
		{
			plugin->init(updatedCfg, (OUTPUT_HANDLE *)this, filterReadingSetFn(useFilteredData));
		}
		m_pipelineManager->registerCategory(m_filters[i], plugin);
	}

	if (!rval)
	{
		// Cleanup possible partly loaded pipeline
	}
	return rval;
}

/**
 * Load a given filter plugin reading for use in the pipeline execution
 *
 * @param filterName		The name of the filter to load
 * @return PLUGIN_HANDLE	Handle on a filter plugin
 */
PLUGIN_HANDLE
PipelineExecutionContext::loadFilterPlugin(const string& filterName)
{
	if (filterName.empty())
	{
		m_logger->error("Unable to fetch filter plugin '%s' .",
			filterName.c_str());
		// Failure
		return NULL;
	}
	m_logger->debug("Loading filter plugin '%s'.", filterName.c_str());

	PluginManager* manager = PluginManager::getInstance();
	PLUGIN_HANDLE handle;
	if ((handle = manager->loadPlugin(filterName, PLUGIN_TYPE_FILTER)) != NULL)
	{
		// Suceess
		m_logger->info("Loaded filter plugin '%s'.", filterName.c_str());
	}
	return handle;
}


/**
 * Do the actual work of filtering the control request. At this point the
 * control request has been transformed into a reading that can be sent via
 * the same set of filters used in south and north services.
 *
 * @param reading	The control request transformed into a reading
 * @return Reading*	The filtered control request. This may be NULL if the filter removes the control request
 */
Reading *PipelineExecutionContext::filter(Reading *reading)
{
	/*
	 * Only one execution at a time per instance as the result
	 * will appear in the member variable m_result
	 */
	lock_guard<mutex> guard(m_mutex);

	if (m_plugins.size() == 0)
		loadPipeline();

	if (m_plugins.size() == 0)
	{
		m_logger->warn("Failed to load the pipeline %s, no filter is configured for the pipeline.", m_name.c_str());
		return NULL;
	}

	/*
	 * Create the reading set from the reading
	 */
	vector<Reading *> *vec = new vector<Reading *>();
	vec->push_back(reading);
	READINGSET *readingSet = new READINGSET(vec);

	/*
	 * Pass the data through the pipeline
	 */
	m_logger->debug("Filtering control request for piepline '%s', %s", m_name.c_str(), reading->toJSON().c_str());
	m_plugins[0]->ingest(readingSet);

	if (m_result && m_result->getCount() > 0)
	{
		Reading *result = m_result->getAllReadings()[0];
		m_result->clear();
		m_logger->debug("Result of filtering control request is %s", result->toJSON().c_str());
		return result;
	}
	m_logger->info("Control filter pipeline %s removed control request", m_name.c_str());
	return NULL;
}



/**
 * Pass the current readings set to the next filter in the pipeline
 *
 * Note:
 * This routine must be passed to all filters "plugin_init" except the last one
 *
 * Static method
 *
 * @param outHandle     Pointer to next filter
 * @param readings      Current readings set
 */
void PipelineExecutionContext::passToOnwardFilter(OUTPUT_HANDLE *outHandle,
				READINGSET *readingSet)
{
	// Get next filter in the pipeline
	FilterPlugin *next = (FilterPlugin *)outHandle;
	// Pass readings to next filter
	next->ingest(readingSet);
}

/**
 * Use the current input readings (they have been filtered
 * by all filters)
 *
 * The assumption is that one of two things has happened.
 *
 *	1. The filtering has all been done in place. In which case
 *	the m_data vector is in the ReadingSet passed in here.
 *
 *	2. The filtering has created new ReadingSet in which case
 *	the reading vector must be copied into m_data from the
 *	ReadingSet.
 *
 * Note:
 * This routine must be passed to last filter "plugin_init" only
 *
 * Static method
 *
 * @param outHandle     Pointer to Ingest class instance
 * @param readingSet    Filtered reading set 
 */
void PipelineExecutionContext::useFilteredData(OUTPUT_HANDLE *outHandle,
			     READINGSET *readingSet)
{
	PipelineExecutionContext *context = (PipelineExecutionContext *)outHandle;
	context->setResult(readingSet);
}

/**
 * Add a new filter into an exisitng pipeline
 *
 * @param filter	The name of the filter to add to the pipeline
 * @param order		The location in the pipeline for the new filter
 */
void PipelineExecutionContext::addFilter(const string& filter, int order)
{
	FilterPlugin *currentPlugin = NULL;

	// Stop ingestion while adding new filter into pipeline
	lock_guard<mutex> guard(m_mutex);

	// Add the filter into the pipeline vector
	auto it = m_filters.begin();
	it += (order - 1);
	m_filters.insert(it, filter);

	// Get the category with values and defaults
	ConfigCategory config = m_management->getCategory(filter);
	if (config.itemExists("plugin"))
	{
		string filterName = config.getValue("plugin");
		m_logger->debug("Loading the plugin '%s' for filter %s", filterName.c_str(), filter.c_str());
		PLUGIN_HANDLE filterHandle;
		// Load filter plugin to obtain a handle that we can use to call the plugin
		filterHandle = loadFilterPlugin(filterName);
		if (!filterHandle)
		{
			string errMsg("Cannot load filter plugin '" + filterName + "'");
			m_logger->error(errMsg.c_str());
			return;
		}
		else
		{
			PluginManager *pluginManager = PluginManager::getInstance();
			// Get plugin default configuration
			string filterConfig = pluginManager->getInfo(filterHandle)->config;

			// Create/Update default filter category items
			DefaultConfigCategory filterDefConfig(filter, filterConfig);
			string filterDescription = "Configuration of '" + filterName;
			filterDescription += "' filter for plugin '" + filter + "'";
			filterDefConfig.setDescription(filterDescription);

			if (!m_management->addCategory(filterDefConfig, true))
			{
				string errMsg("Cannot create/update '" + \
					      filter + "' filter category");
				m_logger->error(errMsg.c_str());
				return;
			}
			else
			{

				// Instantiate the FilterPlugin class
				// in order to call plugin entry points
				currentPlugin = new FilterPlugin(filterName, filterHandle);

				// Add filter to filters vector
				auto it = m_plugins.begin();
				it += (order - 1);
				m_plugins.insert(it, currentPlugin);
			}
		}
	}

	if (!currentPlugin)
	{
		return;
	}

	// The filter has been created now we must "re-plumb" the pipeline
	ConfigCategory updatedCfg = m_management->getCategory(filter);
	if (order < m_plugins.size() - 1)
		currentPlugin->init(updatedCfg, m_plugins[order + 1], filterReadingSetFn(passToOnwardFilter));
	else
		currentPlugin->init(updatedCfg, (OUTPUT_HANDLE *)this, filterReadingSetFn(useFilteredData));
	m_pipelineManager->registerCategory(filter, currentPlugin);

	// Make the filter before the new one send to our new filter
	// To do this we need to call the init routine again, but under
	// the rules of the filter API the filter must be first shutdown
	if (m_plugins.size() > 1)
	{
		FilterPlugin *previous = m_plugins[order - 1];
		previous->shutdown();
		ConfigCategory prevConfig = m_management->getCategory(m_filters[order - 1]);
		previous->init(prevConfig, currentPlugin, filterReadingSetFn(passToOnwardFilter));
	}
}

/**
 * Remove a filter from an existing pipeline
 *
 * @param filter	The name of the filter to remove from the pipeline
 */
void PipelineExecutionContext::removeFilter(const string& filter)
{
	// Remove filter from m_filters collection
	auto it = std::find(m_filters.begin(), m_filters.end(), filter);
	int index = it-m_filters.begin();
	if (it != m_filters.end())
	{
		m_filters.erase(it);

	}

	// Unregister the filter plugin from pipeline and remove the filter plugin from m_plugins collection
	m_plugins[index]->shutdown();
	m_pipelineManager->unregisterCategory(filter, m_plugins[index]);
	m_plugins.erase(m_plugins.begin()+index);
	delete (m_plugins[index]);

	// "re-plumb" the pipeline after deletion
	if (index >= m_plugins.size())
	{
		FilterPlugin *previous = m_plugins[index - 1];
		previous->shutdown();
		ConfigCategory prevConfig = m_management->getCategory(m_filters[index - 1]);
		previous->init(prevConfig, (OUTPUT_HANDLE *)this, filterReadingSetFn(useFilteredData));
	}
}
