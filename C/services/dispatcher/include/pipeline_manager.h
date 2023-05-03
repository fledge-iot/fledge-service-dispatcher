#ifndef _PIPELINE_MANAGER_H
#define _PIPELINE_MANAGER_H
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
#include <management_client.h>
#include <logger.h>
#include <storage_client.h>
#include <map>

#define PIPELINES_TABLE		"control_pipelines"
#define PIPELINES_FILTER_TABLE	"control_filters"
#define SOURCES_TABLE		"control_source"
#define DESTINATIONS_TABLE	"control_destination"

class ControlPipeline;

/**
 * Class to encapsulate the endpoint of a control pipeline
 */
class PipelineEndpoint {
	public:
		/**
		 * The types of end points to which a control pipeline
		 * can be attached.
		 */
		enum		EndpointType {
					EndpointUndefined,
					EndpointAny,
					EndpointService,
					EndpointAPI,
					EndpointNotification,
					EndpointSchedule,
					EndpointScript,
					EndpointBroadcast,
					EndpointAsset
				};

		/**
		 * Default contructor with an undefined endpoint
		 */
		PipelineEndpoint() : m_type(EndpointUndefined) {};

		/**
		 * Create a pipeline end point with an associated name.
		 *
		 * @param type	The type of the endpoint
		 * @param name	The name of the endpoint
		 */
		PipelineEndpoint(EndpointType type, const std::string& name) :
		       			m_type(type), m_name(name)
		{
		};

		/**
		 * Create a pipeline end point which has no name associated
		 * with it.
		 *
		 * The type should be one of EndpointAny, EndpointAPI
		 * or EndpointBroadcast.
		 *
		 * @param type	The type of the endpoint
		 */
		PipelineEndpoint(EndpointType type) : m_type(type)
		{
			if (type != EndpointAny && type != EndpointAPI
					&& type != EndpointBroadcast)
			{
				throw std::runtime_error("Invalid end point type, type must have a name associated");
			}
		};
	
		/**
		 * Match the endpoint against a candidate
		 *
		 * @param candidate	A candidate to match
		 * @return bool		True if the candidate matches
		 */
		bool		match(const PipelineEndpoint& candidate) const
				{
					return candidate.m_type == m_type && (candidate.m_name.empty()
						|| candidate.m_name.compare(m_name) == 0);
				};

	private:
		EndpointType	m_type; // The type of this end point
		std::string	m_name; // The name of the endpoint. Only populated for soem types
};

/**
 * The singleton control pipeline manager class
 *
 * This class is responsible for mannaging the pipelines
 * used in the control dispatcher. It provides
 *
 *   - The means to find an eligible pipeline to be excuted
 *   - The configuration of the pipelines themselves
 *   - The interface to the pipeline storage such that new pipelines
 *     are discovered and changes in pipelines are actioned
 */
class ControlPipelineManager {
	public:
		ControlPipelineManager(ManagementClient *mgtClient, StorageClient *storage);
		~ControlPipelineManager();
		void			loadPipelines();
		ControlPipeline		*findPipeline(const PipelineEndpoint& source, const PipelineEndpoint& dest);

		/**
		 * Return a point to the management client
		 */
		ManagementClient	*getManagementClient()
					{
						return m_managementClient;
					}
	private:
		void			loadLookupTables();
		void			loadFilters(const std::string& pipeline, int cpid, std::vector<std::string>& filters);
		PipelineEndpoint::EndpointType
					findType(const std::string& typeName, bool source);
	public:
		class EndpointLookup {
			public:
				EndpointLookup() : m_type(PipelineEndpoint::EndpointAny) {};
				EndpointLookup(const std::string& name, const std::string& description, PipelineEndpoint::EndpointType type);
				EndpointLookup(const ControlPipelineManager::EndpointLookup& rhs);
				std::string	m_name;
				std::string	m_description;
				PipelineEndpoint::EndpointType
						m_type;
		};
	private:
		Logger			*m_logger;
		std::map<std::string, ControlPipeline *>
					m_pipelines;
		ManagementClient	 *m_managementClient;
		StorageClient		*m_storage;
		std::map<int, ControlPipelineManager::EndpointLookup>
					m_sourceTypes;
		std::map<int, ControlPipelineManager::EndpointLookup>
					m_destTypes;
};

#endif
