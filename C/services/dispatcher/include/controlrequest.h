#ifndef _CONTROL_REQUEST_H
#define _CONTROL_REQUEST_H
/*
 * Fledge Dispatcher service.
 *
 * Copyright (c) 2021 Dianomic Systems
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Mark Riddoch, Massimiliano Pinto
 *
 * A set of classes that implement the storage and execution for
 * the various control requests that can be processed by the control
 * dispatcher micro service.
 */
#include <string>
#include <kvlist.h>
#include <pipeline_manager.h>

class DispatcherService;

/**
 * The base control request class used to queue the control requests for
 * execution
 */
class ControlRequest {
	public:
		virtual void execute(DispatcherService *) = 0;
		virtual PipelineEndpoint getDestination() = 0;

		/**
		 * Set the source name from the authentication sent for
		 * the caller. Note this is only done if the call is 
		 * configured to be authenticated.
		 *
		 * @param  source_name	The name of the authenticated caller
		 */
		void 	setSourceName(const std::string& source_name)
		{
			m_source_name = source_name;
		};

		/**
		 * Set the source type from the authentication sent for
		 * the caller. Note this is only done if the call is 
		 * configured to be authenticated.
		 *
		 * @param source_type	The type of the authenticated caller
		 */
		void 	setSourceType(const std::string& source_type)
		{
			m_source_type = source_type;
		};

		/**
		 * Set the autheneticated requesting URL of
		 * the caller. Note this is only done if the call is 
		 * configured to be authenticated.
		 *
		 * @param source_type	The type of the authenticated caller
		 */
		void    setRequestURL(const std::string& url)
                {
                        m_request_url = url;
                };

		/**
		 * Add the caller information from the request. Used to
		 * match the control pipeline.
		 *
		 * @param	type		The type of the caller
		 * @param	name		The name of the caller
		 */
		void addCaller(const std::string& type, const std::string& name)
		{
			m_callerType = type;
			m_callerName = name;
		};

		PipelineEndpoint	getSource();

	public:
		std::string	m_source_name;
		std::string	m_source_type;
		std::string	m_request_url;
		std::string	m_callerType;
		std::string	m_callerName;
};

/**
 * A generic write request
 */
class WriteControlRequest : public ControlRequest {
	public:
		WriteControlRequest(KVList& values) : m_values(values)
		{
		};
		virtual void execute(DispatcherService *) = 0;
	protected:
		void	     filter(ControlPipelineManager *manager);
	protected:
		KVList				m_values;
};

/**
 * A request to write a control message to a specific service
 */
class ControlWriteServiceRequest : public WriteControlRequest {
	public:
		ControlWriteServiceRequest(const std::string& service, KVList& values) :
		       			m_service(service),
					WriteControlRequest(values)
		{
		};
		void		execute(DispatcherService *);

		/**
		 * Return the endpoint information for the control request
		 *
		 * @return PipelineEndpoint	The destination endpoint of this request
		 */
		PipelineEndpoint
				getDestination()
				{
					return PipelineEndpoint(PipelineEndpoint::EndpointService, m_service);
				};
	private:
		std::string	m_service;
};

/**
 * A request to write a control message to the service that ingests a specific asset
 */
class ControlWriteAssetRequest : public WriteControlRequest {
	public:
		ControlWriteAssetRequest(const std::string& asset, KVList& values) :
		       			m_asset(asset),
					WriteControlRequest(values)
		{
		};
		void		execute(DispatcherService *);

		/**
		 * Return the endpoint information for the control request
		 *
		 * @return PipelineEndpoint	The destination endpoint of this request
		 */
		PipelineEndpoint
				getDestination()
				{
					return PipelineEndpoint(PipelineEndpoint::EndpointAsset, m_asset);
				};
	private:
		std::string	m_asset;
};

/**
 * A request to write a value using a specific script
 */
class ControlWriteScriptRequest : public WriteControlRequest {
	public:
		ControlWriteScriptRequest(const std::string& script, KVList& values) :
					m_scriptName(script),
					WriteControlRequest(values)
		{
		};
		void		execute(DispatcherService *);

		/**
		 * Return the endpoint information for the control request
		 *
		 * @return PipelineEndpoint	The destination endpoint of this request
		 */
		PipelineEndpoint
				getDestination()
				{
					return PipelineEndpoint(PipelineEndpoint::EndpointScript, m_scriptName);
				};
	private:
		std::string	m_scriptName;
};

/**
 * A request to write a value to all south services
 */
class ControlWriteBroadcastRequest : public WriteControlRequest {
	public:
		ControlWriteBroadcastRequest(KVList& values) :
						WriteControlRequest(values)
		{
		};
		void		execute(DispatcherService *);

		/**
		 * Return the endpoint information for the control request
		 *
		 * @return PipelineEndpoint	The destination endpoint of this request
		 */
		PipelineEndpoint
				getDestination()
				{
					return PipelineEndpoint(PipelineEndpoint::EndpointBroadcast);
				};
};

/**
 * A generic operation control request
 */
class ControlOperationRequest : public ControlRequest {
	public:
		ControlOperationRequest(const std::string& operation, KVList& parameters) :
			m_operation(operation), m_parameters(parameters)
		{
		};
		virtual void	execute(DispatcherService *) = 0;

	protected:
		void		filter(ControlPipelineManager *manager);
	protected:
		std::string			m_operation;
		KVList				m_parameters;
};

/**
 * A request to execute an operation on a speified service
 */
class ControlOperationServiceRequest : public ControlOperationRequest {
	public:
		ControlOperationServiceRequest(const std::string& operation, 
				const std::string& service, KVList& parameters):
					m_service(service),
					ControlOperationRequest(operation, parameters)
		{
		};
		void execute(DispatcherService *);

		/**
		 * Return the endpoint information for the control request
		 *
		 * @return PipelineEndpoint	The destination endpoint of this request
		 */
		PipelineEndpoint
				getDestination()
				{
					return PipelineEndpoint(PipelineEndpoint::EndpointService, m_service);
				};

	protected:
		const std::string	m_service;
};

/**
 * A request to execute an operation on a service responsible for the ingest to a given asset
 */
class ControlOperationAssetRequest : public ControlOperationRequest {
	public:
		ControlOperationAssetRequest(const std::string& operation, 
				const std::string& asset, KVList& parameters):
					m_asset(asset),
					ControlOperationRequest(operation, parameters)
		{
		};
		void execute(DispatcherService *);

		/**
		 * Return the endpoint information for the control request
		 *
		 * @return PipelineEndpoint	The destination endpoint of this request
		 */
		PipelineEndpoint
				getDestination()
				{
					return PipelineEndpoint(PipelineEndpoint::EndpointAsset, m_asset);
				};

	protected:
		const std::string	m_asset;
};

/**
 * A request to broadcast an operation to all south services
 */
class ControlOperationBroadcastRequest : public ControlOperationRequest {
	public:
		ControlOperationBroadcastRequest(const std::string& operation, KVList& parameters) :
					ControlOperationRequest(operation, parameters)
		{
		};
		void execute(DispatcherService *);

		/**
		 * Return the endpoint information for the control request
		 *
		 * @return PipelineEndpoint	The destination endpoint of this request
		 */
		PipelineEndpoint
				getDestination()
				{
					return PipelineEndpoint(PipelineEndpoint::EndpointBroadcast);
				};
};
#endif
