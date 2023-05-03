#ifndef _CONTROL_PIPELINE_H
#define _CONTROL_PIPELINE_H
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
#include <string>
#include <vector>
#include <pipeline_manager.h>

class PipelineExecutionContext;

/**
 * A class that encapsulates the source and destination endpoints of
 * a control pipeline with its execution contenxt
 */
class ContextEndpoints {
	public:
		/**
		 * Construct a pipelines end point pair and context
		 *
		 * @param context	The pipeline execution context
		 * @param source	The source endpoint
		 * @param dest		The destination endpoint
		 */
		ContextEndpoints(PipelineExecutionContext *context, const PipelineEndpoint& source, const PipelineEndpoint& dest) :
			m_context(context), m_source(source), m_dest(dest)
		{
		};

		/**
		 * Construct a pipelines end point pair
		 *
		 * @param source	The source endpoint
		 * @param dest		The destination endpoint
		 */
		ContextEndpoints(const PipelineEndpoint& source, const PipelineEndpoint& dest) :
			m_context(NULL), m_source(source), m_dest(dest)
		{
		};

		~ContextEndpoints();
		void		setContext(PipelineExecutionContext *context);

		/**
		 * Get the Pipeline Execution Context
		 *
		 * @return PipelineExecutionContext*	Return the pipelien execution context
		 */
		PipelineExecutionContext
				*getContext()
				{
					return m_context;
				}

		/**
		 * Allow a comparison of endpoints
		 *  @param rhs	The PipelineEndpoints we are comparing
		 *  @return bool	True if the pipeline endpoints match
		 */
		bool		operator==(const ContextEndpoints rhs) const
				{
					return m_source.match(rhs.m_source) && m_dest.match(rhs.m_dest);
				}

	private:
		PipelineEndpoint		m_source;
		PipelineEndpoint		m_dest;
		PipelineExecutionContext	*m_context;
};

/**
 * Encapsulation of a control pipeline
 */
class ControlPipeline {
	public:
		ControlPipeline(ControlPipelineManager *manager, const std::string& name);
		~ControlPipeline();

		/**
		 * Set the enabled state of the pipeline
		 *
		 * @param enable	The required enabled state
		 */
		void			enable(bool enable)
					{
						m_enable = enable;
					};

		/**
		 * Set the exclusive execution state of the pipeline
		 *
		 * @param exclusive	The required exclusive state of the pipeline
		 */
		void			exclusive(bool exclusive)
					{
						m_exclusive = exclusive;
					}

		/**
		 * Set the endpoints of the pipeline
		 *
		 * @param source	The source that can use this pipeline
		 * @param dest		The destiantion that can use this pipeline
		 */
		void			endpoints(const PipelineEndpoint& source, const PipelineEndpoint& dest)
					{
						m_source = source;
						m_dest = dest;
					}

		/**
		 * Set the filters in the pipeline
		 *
		 * @param pipeline	Ordered vector of the filter names within the pipeline
		 */
		void			setPipeline(const std::vector<std::string>& pipeline)
					{
						m_pipeline = pipeline;
					}

		/**
		 * Determine if the pipeline source and destination match the
		 * required source and destination passed in
		 *
		 * @param source	The source endpoint to match with
		 * @param dest		The destination endpoint to match with
		 * @return bool		Returns true if the source and destination match
		 */
		bool			match(const PipelineEndpoint& source, const PipelineEndpoint& dest)
					{
						return source.match(m_source) && dest.match(m_dest);
					}

		/**
		 * Return the set pipeline as a set of filter names
		 *
		 * @return vector	A const reference to a vector of strings.
		 */
		const std::vector<std::string>&
					getpipeline() const
					{
						return m_pipeline;
					}

		/**
		 * Return the name of the pipeline
		 *
		 * @return string	A reference to the name of the plugin
		 */
		const std::string&	getName()
					{
						return m_name;
					}

		PipelineExecutionContext
					*getExecutionContext(const PipelineEndpoint& source,
							const PipelineEndpoint& dest);
	private:
		std::string		m_name;
		bool			m_enable;
		bool			m_exclusive;
		PipelineEndpoint	m_source;
		PipelineEndpoint	m_dest;
		std::vector<std::string>
					m_pipeline;
		PipelineExecutionContext
					*m_sharedContext;
		std::vector<ContextEndpoints>
					m_contexts;
		ControlPipelineManager	*m_manager;
};

#endif
