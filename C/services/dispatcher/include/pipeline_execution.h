#ifndef _PIPELINE_CONTEXT_H
#define _PIPELINE_CONTEXT_H
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
#include <logger.h>
#include <vector>
#include <string>
#include <management_client.h>
#include <plugin.h>
#include <reading_set.h>
#include <plugin.h>
#include <filter_plugin.h>
#include <mutex>

class ControlPipelineManager;
class FilterPlugin;
class Reading;

typedef void (*filterReadingSetFn)(OUTPUT_HANDLE *outHandle, READINGSET* readings);

/**
 * A class that encapsulates the execution context of a control filter pipeline.
 * The context may be shared between different control flows or may be unique
 * to a flow from a given source to destination.
 */
class PipelineExecutionContext {
	public:
		PipelineExecutionContext(ManagementClient *management, const std::string& name, const std::vector<std::string>& filters);
		~PipelineExecutionContext();
		void				setPipelineManager(ControlPipelineManager *manager) { m_pipelineManager = manager; };

		Reading				*filter(Reading *reading);

						/**
						 * Set the result of the pipeline execution
						 *
						 * @param result	The output of the filter pipeline
						 */
		void				setResult(READINGSET *result)
						{
							m_result = result;
						};
		void				addFilter(const std::string& filter, int order);
		void				removeFilter(const std::string& filter);
		void				reorder(const std::string& filter, int order);
	private:
		void				rePlumbFilters();
		bool				loadPipeline();
		PLUGIN_HANDLE			loadFilterPlugin(const std::string& filterName);
		void				shutdownPlugin(FilterPlugin *plugin);

		static void			passToOnwardFilter(OUTPUT_HANDLE *outHandle, READINGSET* readings);
		static void			useFilteredData(OUTPUT_HANDLE *outHandle, READINGSET* readings);

	private:
		std::string			m_name;
		Logger				*m_logger;
		ManagementClient		*m_management;
		std::vector<std::string>	m_filters;
		std::vector<FilterPlugin *>	m_plugins;
		READINGSET			*m_result;
		std::mutex			m_mutex;
		ControlPipelineManager		*m_pipelineManager;
};

#endif
