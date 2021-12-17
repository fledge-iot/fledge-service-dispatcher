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
#include <vector>

class DispatcherService;

/**
 * The base control request class used to queue the control requests for
 * execution
 */
class ControlRequest {
	public:
		virtual void execute(DispatcherService *) = 0;
};

/**
 * A generic write request
 */
class WriteControlRequest : public ControlRequest {
	public:
		WriteControlRequest(const std::string& key, const std::string& value) :
			m_key(key), m_value(value)
		{
		};
		virtual void execute(DispatcherService *) = 0;
	protected:
		const std::string		m_key;
		const std::string		m_value;
};

/**
 * A request to write a control message to a specific service
 */
class ControlWriteServiceRequest : public WriteControlRequest {
	public:
		ControlWriteServiceRequest(const std::string& service,
				const std::string& key,
				const std::string& value) : m_service(service),
					WriteControlRequest(key, value)
		{
		};
		void		execute(DispatcherService *);
	private:
		std::string	m_service;
};

/**
 * A request to write a value usign a specific script
 */
class ControlWriteScriptRequest : public WriteControlRequest {
	public:
		ControlWriteScriptRequest(const std::string& script,
				const std::string& key,
				const std::string& value) : m_scriptName(script),
					WriteControlRequest(key, value)
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
		ControlWriteBroadcastRequest( const std::string& key,
					const std::string& value) :
						WriteControlRequest(key, value)
		{
		};
		void		execute(DispatcherService *);
};

/**
 * A generic operation control request
 */
class ControlOperationRequest : public ControlRequest {
	public:
		ControlOperationRequest(const std::string& operation) :
			m_operation(operation)
		{
		};
		ControlOperationRequest(const std::string& operation,
				std::vector<std::pair<std::string, std::string> > parameters);
		virtual void execute(DispatcherService *) = 0;
	protected:
		const std::string		m_operation;
		std::vector<std::pair<std::string, std::string> >
						m_parameters;
};

/**
 * A request to execute an operation on a speified service
 */
class ControlOperationServiceRequest : public ControlOperationRequest {
	public:
		ControlOperationServiceRequest(const std::string& operation, 
				const std::string& service) : m_service(service),
					ControlOperationRequest(operation)
		{
		};
		ControlOperationServiceRequest(const std::string& operation, 
				const std::string& service,
				std::vector<std::pair<std::string, std::string> > parameters) :
					m_service(service),
					ControlOperationRequest(operation)
		{
		};
		void execute(DispatcherService *);

	protected:
		const std::string	m_service;
};

/**
 * A request to broadcast an operation to all south services
 */
class ControlOperationBroadcastRequest : public ControlOperationRequest {
	public:
		ControlOperationBroadcastRequest(const std::string& operation) :
					ControlOperationRequest(operation)
		{
		};
		ControlOperationBroadcastRequest(const std::string& operation,
				std::vector<std::pair<std::string, std::string> > parameters) :
					ControlOperationRequest(operation)
		{
		};
		void execute(DispatcherService *);
};
#endif
