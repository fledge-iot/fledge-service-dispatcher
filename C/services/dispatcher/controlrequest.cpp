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

using namespace std;

/**
 * Implementation of the write to a partcular service from the dispatcher
 * service.
 */
void ControlWriteServiceRequest::execute(DispatcherService *service)
{
	string payload = "{ \"values\" : { \"";
	payload += m_key;
	payload += "\" : \"";
	payload += m_value;
	payload += "\" } }";
	service->sendToService(m_service, "/fledge/south/setpoint", payload);
}

/**
 * Implementation of the execution of a broadcast write request
 */
void ControlWriteBroadcastRequest::execute(DispatcherService *service)
{
}

/**
 * Implementation of the execution of a script write request
 */
void ControlWriteScriptRequest::execute(DispatcherService *service)
{
}
