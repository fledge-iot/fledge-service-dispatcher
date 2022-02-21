#ifndef _CONTROL_REQUEST_H
#define _CONTROL_REQUEST_H
/*
 * Fledge Dispatcher service.
 *
 * Copyright (c) 2021 Dianomic Systems
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Mark Riddoch
 *
 * A set of classes thatg implement the storage and execution for
 * the various control requests that can be processed by the control
 * dispatcher micro service.
 */
#include <string>
#include <kvlist.h>

class DispatcherService;

/**
 * The base control request class used to queue the control requests for
 * execution
 */
class ControlRequest {
	public:
		virtual void execute(DispatcherService *) = 0;

		void 	setSourceName(std::string& source_name)
		{
			m_source_name = source_name;
		};
		void 	setSourceType(std::string& source_type)
		{
			m_source_type = source_type;
		};

	public:
		std::string	m_source_name;
		std::string	m_source_type;
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
		const std::string		m_operation;
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

	protected:
		const std::string	m_service;
};

/**
 * A request to execute an operation on a service responsible for the ingest o a given asset
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
};
#endif
