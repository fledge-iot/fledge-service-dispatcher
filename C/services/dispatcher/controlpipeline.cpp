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
#include <controlpipeline.h>
#include <pipeline_manager.h>
#include <pipeline_execution.h>
#include <algorithm>

using namespace std;

/**
 * Constructor for the Control Pipeline class
 *
 * @param name	The name of the control pipeline
 */
ControlPipeline::ControlPipeline(ControlPipelineManager *manager, const string& name) : m_name(name),
	m_enable(true), m_exclusive(false), m_sharedContext(NULL), m_manager(manager)
{
	m_logger = Logger::getLogger();
}

/**
 * Destructor for the control pipeline
 */
ControlPipeline::~ControlPipeline()
{
	if (m_sharedContext)
		delete m_sharedContext;
	m_sharedContext = NULL;
}

/**
 * Return an execution context that can be used to execute the pipeline
 *
 * @param source	The pipeline endpoint source
 * @param dest		The pipeline endpoint destination
 * @return PipelineExecutionContext*	The contest to execute the pipeline within
 */
PipelineExecutionContext *
ControlPipeline::getExecutionContext(const PipelineEndpoint& source, const PipelineEndpoint& dest)
{
	lock_guard<mutex> guard(m_contextMutex);	// Stop the context beign handed out
	if (!m_exclusive)
	{
		if (!m_sharedContext)
		{
			m_sharedContext = new PipelineExecutionContext(m_manager->getManagementClient(), m_name, m_pipeline);
			m_sharedContext->setPipelineManager(m_manager);
		}
		m_logger->info("Using shared context for control pipeline '%s' from '%s' to '%s'",
				m_name.c_str(), source.toString().c_str(), dest.toString().c_str());
		return m_sharedContext;
	}

	// We need an exclusive context for this source/destination pair
	ContextEndpoints ends(source, dest);
	PipelineExecutionContext *context = NULL;
	for (auto& it : m_contexts)
	{
		if (it == ends)
		{
			return it.getContext();
		}
	}
	m_logger->info("Create new context to run pipeline '%s' between '%s' and '%s'",
			m_name.c_str(), source.toString().c_str(), dest.toString().c_str());
	context = new PipelineExecutionContext(m_manager->getManagementClient(), m_name, m_pipeline);
	context->setPipelineManager(m_manager);
	ends.setContext(context);
	m_contexts.push_back(ends);
	return context;
}

/**
 * Add a new filter into an exisitng pipeline
 *
 * @param filter	The name of the filter to add to the pipeline
 * @param order		The location in the pipeline for the new filter
 */
void ControlPipeline::addFilter(const string& filter, int order)
{
	// Add the filter into the pipeline vector
	lock_guard<mutex> guard(m_contextMutex);	// Stop the context being handed out

	auto it = m_pipeline.begin();
	it += (order - 1);
	m_pipeline.insert(it, filter);
	
	// Update the contexts that exist for the pipeline
	if (m_sharedContext)
	{
		m_sharedContext->addFilter(filter, order);
	}
	for (auto &ends : m_contexts)
	{
		ends.getContext()->addFilter(filter, order);
	}
}

/**
 * Remove the named filter from the pipeline
 *
 * @param filter	The name of the filter to remove
 */
void ControlPipeline::removeFilter(const string& filter)
{
	lock_guard<mutex> guard(m_contextMutex);

	auto it = std::find(m_pipeline.begin(), m_pipeline.end(), filter);
	if(it != m_pipeline.end())
	{
		m_pipeline.erase(it);
	}

	if (m_sharedContext)
	{
		m_sharedContext->removeFilter(filter);
	}
	for (auto &ends : m_contexts)
	{
		ends.getContext()->removeFilter(filter);
	}
}

/**
 * Reorder the named filter within the pipeline
 *
 * @param filter	The name of the filter to reorder
 * @param order		The required position in the pipeline
 */
void ControlPipeline::reorder(const string& filter, int order)
{
	if (m_pipeline[order - 1].compare(filter) == 0)
	{
		// Already in the correct location. This can happen
		// as when two filters move position we get an update
		// for both but the first update will correct the second
		// filter as well
		return;
	}

	// An update is required
	lock_guard<mutex> guard(m_contextMutex);

	for (int currentPosition = 0; currentPosition < m_pipeline.size(); currentPosition++)
	{
		if (m_pipeline[currentPosition].compare(filter) == 0)
		{
			m_pipeline[currentPosition] = m_pipeline[order - 1];
			m_pipeline[order - 1] = filter;

			if (m_sharedContext)
			{
				m_sharedContext->reorder(filter, order);
			}
			for (auto &ends : m_contexts)
			{
				ends.getContext()->reorder(filter, order);
			}
			return;
		}
	}
	m_logger->error("Failed to find filter %s in pipeline %s to re-order",
				filter.c_str(), m_name.c_str());
}

/**
 * Remove all the contexts that exist for this pipeline
 */
void ControlPipeline::removeAllContexts()
{
	lock_guard<mutex> guard(m_contextMutex);
	if (m_sharedContext)
	{
		delete m_sharedContext;
	}
	m_sharedContext = NULL;
	m_contexts.clear();
}

/*
 * Destructor for the pipeline execution context endpoints
 */
ContextEndpoints::~ContextEndpoints()
{
	if (m_context)
		delete m_context;
}

/**
 * Set the PipelineExecutionContext for the endpoints
 *
 * @param context	The pipeline execution context
 */
void ContextEndpoints::setContext(PipelineExecutionContext *context)
{
	if (m_context)
		delete m_context;
	m_context = context;
}

