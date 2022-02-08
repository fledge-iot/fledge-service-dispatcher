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
#include <asset_tracking.h>

using namespace std;

/**
 * Implementation of the write to a partcular service from the dispatcher
 * service.
 *
 * @param service	The dispatcher service that provides the methods required
 */
void ControlWriteServiceRequest::execute(DispatcherService *service)
{
	string payload = "{ \"values\" : ";
	payload += m_values.toJSON();
	payload += " }";
	Logger::getLogger()->debug("Send payload to service '%s'", payload.c_str());
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
	service->getMgmtClient()->getServices(services, "Southbound");

	string payload = "{ \"values\" : ";
	payload += m_values.toJSON();
	payload += " }";
	
	for (auto& record : services)
	{
		try {
			service->sendToService(record->getName(), "/fledge/south/setpoint", payload);
		} catch (...) {
			Logger::getLogger()->info("Service '%s' does not support write operation", record->getName().c_str());
		}
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
 * Implementation of the write to the service that ingests a specific asset 
 * from the dispatcher service.
 *
 * @param service	The dispatcher service that provides the methods required
 */
void ControlWriteAssetRequest::execute(DispatcherService *service)
{
	AssetTracker *tracker = AssetTracker::getAssetTracker();
	try {
		string ingestService = tracker->getIngestService(m_asset);
		string payload = "{ \"values\" : ";
		payload += m_values.toJSON();
		payload += " }";
		service->sendToService(ingestService, "/fledge/south/setpoint", payload);
	} catch (...) {
		Logger::getLogger()->error("Unable to fetch service that ingests asset %s",
				m_asset.c_str());
	}
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
		payload += "\"parameters\" : ";
		payload += m_parameters.toJSON();
	}
	payload += " }";
	service->sendToService(m_service, "/fledge/south/operation", payload);
}

/**
 * Implementation of the execution of an operation on a south service responible
 * for the ingest of a given asset
 *
 * @param service	The dispatcher service that provides the methods required
 */
void ControlOperationAssetRequest::execute(DispatcherService *service)
{
	AssetTracker *tracker = AssetTracker::getAssetTracker();
	try {
		string ingestService = tracker->getIngestService(m_asset);
		string payload = "{ \"operation\" : \"";
		payload += m_operation;
		payload += "\", ";
		if (m_parameters.size() > 0)
		{
			payload += "\"parameters\" : ";
			payload += m_parameters.toJSON();
		}
		payload += " }";
		service->sendToService(ingestService, "/fledge/south/operation", payload);
	} catch (...) {
		Logger::getLogger()->error("Unable to fetch service that ingests asset %s",
				m_asset.c_str());
	}
}

/**
 * Implementation of the execution of an operation on all south services
 *
 * @param service	The dispatcher service that provides the methods required
 */
void ControlOperationBroadcastRequest::execute(DispatcherService *service)
{
	vector<ServiceRecord *> services;
	service->getMgmtClient()->getServices(services, PLUGIN_TYPE_SOUTH);

	string payload = "{ \"operation\" : \"";
	payload += m_operation;
	payload += "\", ";
	if (m_parameters.size() > 0)
	{
		payload += "\"parameters\" : ";
		payload += m_parameters.toJSON();
	}
	payload += " }";
	
	for (auto& record : services)
	{
		service->sendToService(record->getName(), "/fledge/south/operation", payload);
	}
}
