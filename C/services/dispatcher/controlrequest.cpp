/*
 * Fledge Dispatcher API class for dispatcher control request classes
 *
 * Copyright (c) 2021 Dianomic Systems
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Mark Riddoch
 */
#include <controlrequest.h>
#include <dispatcher_service.h>
#include <automation.h>
#include <plugin_api.h>

using namespace std;

/**
 * Implementation of the write to a partcular service from the dispatcher
 * service.
 *
 * @param service	The dispatcher service that provides the methods required
 */
void ControlWriteServiceRequest::execute(DispatcherService *service)
{
	string payload = "{ \"values\" : { ";
	payload += m_values.toJSON();
	payload += "\" } }";
	service->sendToService(m_service, "/fledge/south/setpoint", payload);
}

/**
 * Implementation of the execution of a broadcast write request
 *
 * @param service	The dispatcher service that provides the methods required
 */
void ControlWriteBroadcastRequest::execute(DispatcherService *service)
{
	vector<ServiceRecord *> services;
	service->getManagementClient()->getServices(services, PLUGIN_TYPE_SOUTH);

	string payload = "{ \"values\" : { ";
	payload += m_values.toJSON();
	payload += "\" } }";
	
	for (auto& record : services)
	{
		service->sendToService(record->getName(), "/fledge/south/setpoint", payload);
	}
}

/**
 * Implementation of the execution of a script write request
 *
 * @param service	The dispatcher service that provides the methods required
 */
void ControlWriteScriptRequest::execute(DispatcherService *service)
{
	Script script(m_scriptName);
	script.execute(service, m_values);
}


/**
 * Implementation of the execution of an operation on a specified south service
 *
 * @param service	The dispatcher service that provides the methods required
 */
void ControlOperationServiceRequest::execute(DispatcherService *service)
{
	string payload = "{ \"operation\" : \"";
	payload += m_operation;
	payload += "\", ";
	if (m_parameters.size() > 0)
	{
		payload += "\"parameters\" : { ";
		payload += m_parameters.toJSON();
		payload += "} ";
	}
	payload += " }";
	service->sendToService(m_service, "/fledge/south/operation", payload);
}

/**
 * Implementation of the execution of an operation on all south services
 *
 * @param service	The dispatcher service that provides the methods required
 */
void ControlOperationBroadcastRequest::execute(DispatcherService *service)
{
	vector<ServiceRecord *> services;
	service->getManagementClient()->getServices(services, PLUGIN_TYPE_SOUTH);

	string payload = "{ \"operation\" : \"";
	payload += m_operation;
	payload += "\", ";
	if (m_parameters.size() > 0)
	{
		payload += "\"parameters\" : { ";
		payload += m_parameters.toJSON();
		payload += "} ";
	}
	payload += " }";
	
	for (auto& record : services)
	{
		service->sendToService(record->getName(), "/fledge/south/operation", payload);
	}
}
