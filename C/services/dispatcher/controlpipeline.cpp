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

using namespace std;

/**
 * Constructor for the Control Pipeline class
 *
 * @param name	The name of the control pipeline
 */
ControlPipeline::ControlPipeline(ControlPipelineManager *manager, const string& name) : m_name(name),
	m_enable(true), m_exclusive(false), m_sharedContext(NULL), m_manager(manager)
{
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
	if (!m_exclusive)
	{
		if (!m_sharedContext)
		{
			m_sharedContext = new PipelineExecutionContext(m_manager->getManagementClient(), m_name, m_pipeline);
			m_sharedContext->setPipelineManager(m_manager);
		}
		Logger::getLogger()->info("Using shared context for control pipeline '%s' from '%s' to '%s'",
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
	Logger::getLogger()->info("Create new context to run pipeline '%s' between '%s' and '%s'",
			m_name.c_str(), source.toString().c_str(), dest.toString().c_str());
	context = new PipelineExecutionContext(m_manager->getManagementClient(), m_name, m_pipeline);
	context->setPipelineManager(m_manager);
	ends.setContext(context);
	m_contexts.push_back(ends);
	return context;
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

