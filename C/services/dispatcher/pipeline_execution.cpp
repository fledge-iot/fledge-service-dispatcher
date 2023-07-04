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
 */
PipelineExecutionContext::~PipelineExecutionContext()
{
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
					Logger::getLogger()->error(errMsg.c_str());
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
						Logger::getLogger()->fatal(errMsg.c_str());
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
		Logger::getLogger()->info("loadFilters: no filters configured for pipeline %s", m_name.c_str());
		return true;
	}
	catch (exception& e)
	{
		Logger::getLogger()->fatal("loadFilters: failed to handle exception for pipeline %s, %s",
				m_name.c_str(), e.what());
		return false;
	}
	catch (...)
	{
		Logger::getLogger()->fatal("loadFilters: generic exception while loading %s", m_name.c_str());
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
		Logger::getLogger()->error("Unable to fetch filter plugin '%s' .",
			filterName.c_str());
		// Failure
		return NULL;
	}
	Logger::getLogger()->debug("Loading filter plugin '%s'.", filterName.c_str());

	PluginManager* manager = PluginManager::getInstance();
	PLUGIN_HANDLE handle;
	if ((handle = manager->loadPlugin(filterName, PLUGIN_TYPE_FILTER)) != NULL)
	{
		// Suceess
		Logger::getLogger()->info("Loaded filter plugin '%s'.", filterName.c_str());
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
		m_logger->warn("Failed to load pipeline %s", m_name.c_str());
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
