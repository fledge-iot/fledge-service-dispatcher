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
#include <pipeline_execution.h>
#include <controlpipeline.h>

using namespace std;

/**
 * Implementation of the write to a partcular service from the dispatcher
 * service.
 *
 * @param service	The dispatcher service that provides the methods required
 */
void ControlWriteServiceRequest::execute(DispatcherService *service)
{
	filter(service->getPipelineManager());
	string payload = "{ \"values\" : ";
	payload += m_values.toJSON();
	payload += " }";

	Logger::getLogger()->debug("Send payload to service '%s'", payload.c_str());

	// Pass m_source_name & m_source_type to south service
	service->sendToService(m_service,
				"/fledge/south/setpoint",
				payload,
				m_source_name,
				m_source_type);
}

/**
 * Implementation of the execution of a broadcast write request
 *
 * @param service	The dispatcher service that provides the methods required
 */
void ControlWriteBroadcastRequest::execute(DispatcherService *service)
{
	filter(service->getPipelineManager());
	vector<ServiceRecord *> services;
	service->getMgmtClient()->getServices(services, "Southbound");

	string payload = "{ \"values\" : ";
	payload += m_values.toJSON();
	payload += " }";
	
	for (auto& record : services)
	{
		try {
			// Pass m_source_name & m_source_type to south service
			service->sendToService(record->getName(),
						"/fledge/south/setpoint",
						payload,
						m_source_name,
						m_source_type);
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
	filter(service->getPipelineManager());
	Script script(m_scriptName);

	// Set m_source_name, m_source_name and m_request_url in the Script object
	script.setSourceName(m_source_name);
	script.setSourceType(m_source_type);
	script.setRequestURL(m_request_url);

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
	filter(service->getPipelineManager());
	AssetTracker *tracker = AssetTracker::getAssetTracker();
	try {
		string ingestService = tracker->getIngestService(m_asset);
		string payload = "{ \"values\" : ";
		payload += m_values.toJSON();
		payload += " }";

		// Pass m_source_name & m_source_type to south service
		service->sendToService(ingestService,
				"/fledge/south/setpoint",
				payload,
				m_source_name,
				m_source_type);
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
	filter(service->getPipelineManager());
	string payload = "{ \"operation\" : \"";
	payload += m_operation;
	payload += "\", ";
	if (m_parameters.size() > 0)
	{
		payload += "\"parameters\" : ";
		payload += m_parameters.toJSON();
	}
	payload += " }";

	// Pass m_source_name & m_source_type to south service
	service->sendToService(m_service,
				"/fledge/south/operation",
				payload,
				m_source_name,
				m_source_type);
}

/**
 * Implementation of the execution of an operation on a south service responible
 * for the ingest of a given asset
 *
 * @param service	The dispatcher service that provides the methods required
 */
void ControlOperationAssetRequest::execute(DispatcherService *service)
{
	filter(service->getPipelineManager());
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

		// Pass m_source_name & m_source_type to south service
		service->sendToService(ingestService,
					"/fledge/south/operation",
					payload,
					m_source_name,
					m_source_type);
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
	filter(service->getPipelineManager());
	vector<ServiceRecord *> services;
	service->getMgmtClient()->getServices(services, "Southbound");

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
		// Pass m_source_name & m_source_type to south service
		service->sendToService(record->getName(),
					"/fledge/south/operation",
					payload,
					m_source_name,
					m_source_type);
	}
}

/**
 * Pass a write control requests through a control filter pipeline
 * if one has been defined for the the particular control pipeline.
 *
 * This method will look for a best match pipeline for the control request
 * based on the source of the request and the destiantion of the request.
 *
 * If a pipeline is found then it will fetch an execution context for the
 * pipeline and then the write request will be transformed into
 * a reading. That reading will be passed to the pipeline and the result
 * of that pipeline execution turned back into a control request and the
 * request will then proceed as previously.
 *
 * @param manager	The control pipeline manager
 */
void WriteControlRequest::filter(ControlPipelineManager *manager)
{
	PipelineEndpoint destination = getDestination();
	PipelineEndpoint source(PipelineEndpoint::EndpointAny);		// TODO need to get correct source
	ControlPipeline *pipeline = manager->findPipeline(source, destination);
	if (!pipeline)
	{
		// Nothing to do
		return;
	}
	PipelineExecutionContext *context = pipeline->getExecutionContext(source, destination);
	if (!context)
	{
		Logger::getLogger()->error("Unable to allocate an execution context for the control pipeline '%s'",
				pipeline->getName());
		return;
	}
	Reading *reading = m_values.toReading("reading");
	// Filter the reading
	context->filter(reading);
	m_values.fromReading(reading);
	delete reading;
}

/**
 * Pass a control operation through a control filter pipeline if
 * one has been defined for the particular source and destination.
 *
 * This method will look for a best match pipeline for the control request
 * based on the source of the request and the destiantion of the request.
 *
 * If a pipeline is found then it will fetch an execution context for the
 * pipeline and then the write request will be transformed into
 * a reading. That reading will be passed to the pipeline and the result
 * of that pipeline execution turned back into a control request and the
 * request will then proceed as previously.
 *
 * @param manager	The control pipeline manager
 */
void ControlOperationRequest::filter(ControlPipelineManager *manager)
{
	PipelineEndpoint destination = getDestination();
	PipelineEndpoint source(PipelineEndpoint::EndpointAny);		// TODO need to get correct source
	ControlPipeline *pipeline = manager->findPipeline(source, destination);
	if (!pipeline)
	{
		// Nothing to do
		return;
	}
}
