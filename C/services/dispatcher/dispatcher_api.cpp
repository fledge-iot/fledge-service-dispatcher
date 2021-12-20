/*
 * Fledge Dispatcher API class for Dispatcher micro service.
 *
 * Copyright (c) 2021 Dianomic Systems
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Massimiliano Pinto, Mark Riddoch
 */
#include "client_http.hpp"
#include "server_http.hpp"
#include "string_utils.h"
#include "management_api.h"
#include "dispatcher_api.h"
#include <rapidjson/document.h>
#include <dispatcher_service.h>
#include <controlrequest.h>
#include <plugin_api.h>

DispatcherApi* DispatcherApi::m_instance = 0;

using namespace std;
using namespace rapidjson;
using HttpServer = SimpleWeb::Server<SimpleWeb::HTTP>;
using HttpClient = SimpleWeb::Client<SimpleWeb::HTTP>;


/**
 * Construct the singleton Dispatcher API
 *
 * @param    port	Listening port (0 = automatically set)
 * @param    threads	Thread pool size of HTTP server
 */
DispatcherApi::DispatcherApi(const unsigned short port,
				 const unsigned int threads)
{
	m_port = port;
	m_threads = threads;
	m_server = new HttpServer();
	m_server->config.port = port;
	m_server->config.thread_pool_size = threads;
	m_thread = NULL;
	m_logger = Logger::getLogger();
	DispatcherApi::m_instance = this;
}

/**
 * DispatcherAPi destructor
 */
DispatcherApi::~DispatcherApi()
{
	delete m_server;
	delete m_thread;
}

/**
 * Return the singleton instance of the Dispatcher API class
 */
DispatcherApi* DispatcherApi::getInstance()
{
	if (m_instance == NULL)
	{
		m_instance = new DispatcherApi(0, 1);
	}
	return m_instance;
}

/**
 * Return the current listener port
 *
 * @return	The current listener port
 */
unsigned short DispatcherApi::getListenerPort()
{
	return m_server->getLocalPort();
}

/**
 * Method for HTTP server, called by a dedicated thread
 */
void startService()
{
	DispatcherApi::getInstance()->startServer();
}

/**
 * Start the HTTP server
 */
void DispatcherApi::start(DispatcherService *service) {
	m_service = service;
	m_thread = new thread(startService);
}

/**
 * Start method for HTTP server
 */
void DispatcherApi::startServer() {
	m_server->start();
}

/**
 * Stop method for HTTP server
 */
void DispatcherApi::stopServer() {
	m_server->stop();
}

/**
 * API stop entery point
 */
void DispatcherApi::stop()
{
	this->stopServer();
}

/**
 * Wait for the HTTP server to shutdown
 */
void DispatcherApi::wait() {
	m_thread->join();
}

/**
 * Handle a write request call
 */
void DispatcherApi::write(shared_ptr<HttpServer::Response> response,
				      shared_ptr<HttpServer::Request> request)
{
	string destination, name, key, value;
	string payload = request->content.string();
	try {
		Document doc;
		ParseResult result = doc.Parse(payload.c_str());
		if (result)
		{
			if (doc.HasMember("destination") && doc["destination"].IsString())
			{
				destination = doc["destination"].GetString();
			}
			else
			{
				string responsePayload = QUOTE({ "message" : "Missing 'destination' in write payload" });
				respond(response, SimpleWeb::StatusCode::client_error_bad_request,responsePayload);
				return;
			}
			if (doc.HasMember("name") && doc["name"].IsString())
			{
				name = doc["name"].GetString();
			}
			else if (destination.compare("script") == 0)
			{
				string responsePayload = QUOTE({ "message" : "Missing script name in write payload" });
				respond(response, SimpleWeb::StatusCode::client_error_bad_request,responsePayload);
				return;
			}
			else if (destination.compare("service") == 0)
			{
				string responsePayload = QUOTE({ "message" : "Missing service name in write payload" });
				respond(response, SimpleWeb::StatusCode::client_error_bad_request,responsePayload);
				return;
			}
			if (doc.HasMember("write") && doc["write"].IsObject())
			{
				KVList values(doc["write"]);
				ControlRequest *writeRequest = NULL;
				if (destination.compare("service"))
				{
					writeRequest = new ControlWriteServiceRequest(name, values); 
				}
				else if (destination.compare("script"))
				{
					writeRequest = new ControlWriteScriptRequest(name, values); 
				}
				else if (destination.compare("broadcast"))
				{
					writeRequest = new ControlWriteBroadcastRequest(values); 
				}
				if (writeRequest)
				{
					queueRequest(writeRequest);
				}
			}
		}
		else
		{
			string responsePayload = QUOTE({ "message" : "Failed to parse request payload" });
			respond(response, SimpleWeb::StatusCode::client_error_bad_request,responsePayload);
		}
		
	} catch (exception &e) {
		char buffer[80];
		snprintf(buffer, sizeof(buffer), "\"Exception: %s\"", e.what());
		string responsePayload = QUOTE({ "message" : buffer });
		respond(response, SimpleWeb::StatusCode::client_error_bad_request,responsePayload);
	}
}

/**
 * Handle a operation request call
 */
void DispatcherApi::operation(shared_ptr<HttpServer::Response> response,
				      shared_ptr<HttpServer::Request> request)
{
	string payload("{ \"error\" : \"Unsupported URL: " + request->path + "\" }");
	respond(response,
		SimpleWeb::StatusCode::client_error_bad_request,
		payload);
}

/**
 * Handle a bad URL endpoint call
 */
void DispatcherApi::defaultResource(shared_ptr<HttpServer::Response> response,
				      shared_ptr<HttpServer::Request> request)
{
	string payload("{ \"error\" : \"Unsupported URL: " + request->path + "\" }");
	respond(response,
		SimpleWeb::StatusCode::client_error_bad_request,
		payload);
}

/**
 * Wrapper for not handled URLS
 *
 * @param response	The response the should be sent
 * @param request	The API request
 */
static void defaultWrapper(shared_ptr<HttpServer::Response> response,
		    shared_ptr<HttpServer::Request> request)
{
	DispatcherApi *api = DispatcherApi::getInstance();
	api->defaultResource(response, request);
}

/**
 * Wrapper for write operation API entry point
 *
 * @param response	The response the should be sent
 * @param request	The API request
 */
static void writeWrapper(shared_ptr<HttpServer::Response> response,
		    shared_ptr<HttpServer::Request> request)
{
	DispatcherApi *api = DispatcherApi::getInstance();
	api->write(response, request);
}

/**
 * Wrapper for operation API entry point
 *
 * @param response	The response the should be sent
 * @param request	The API request
 */
static void operationWrapper(shared_ptr<HttpServer::Response> response,
		    shared_ptr<HttpServer::Request> request)
{
	DispatcherApi *api = DispatcherApi::getInstance();
	api->operation(response, request);
}

/**
 * Initialise the API entry points for the common data resource and
 * the readings resource.
 */
void DispatcherApi::initResources()
{
	// Handle errors
	m_server->default_resource["GET"] = defaultWrapper;
	m_server->default_resource["POST"] = defaultWrapper;
	m_server->default_resource["DELETE"] = defaultWrapper;
	m_server->resource[DISPATCH_WRITE]["POST"] = writeWrapper;
	m_server->resource[DISPATCH_OPERATION]["POST"] = operationWrapper;
}

/**
 * Handle a exception by sendign back an internal error
 *
  *
 * @param response	The response stream to send the response on.
 * @param ex		The current exception caught.
 */
void DispatcherApi::internalError(shared_ptr<HttpServer::Response> response,
				    const exception& ex)
{
	string payload = "{ \"Exception\" : \"";

	payload = payload + string(ex.what());
	payload = payload + "\"";

	m_logger->error("DispatcherApi Internal Error: %s\n", ex.what());

	this->respond(response,
		      SimpleWeb::StatusCode::server_error_internal_server_error,
		      payload);
}

/**
 * Construct an HTTP response with the 200 OK return code using the payload
 * provided.
 *
 * @param response	The response stream to send the response on
 * @param payload	The payload to send
 */
void DispatcherApi::respond(shared_ptr<HttpServer::Response> response,
			      const string& payload)
{
	*response << "HTTP/1.1 200 OK\r\nContent-Length: "
		  << payload.length() << "\r\n"
		  <<  "Content-type: application/json\r\n\r\n" << payload;
}

/**
 * Construct an HTTP response with the specified return code using the payload
 * provided.
 *
 * @param response	The response stream to send the response on
 * @param code		The HTTP esponse code to send
 * @param payload	The payload to send
 */
void DispatcherApi::respond(shared_ptr<HttpServer::Response> response,
			      SimpleWeb::StatusCode code,
			      const string& payload)
{
	*response << "HTTP/1.1 " << status_code(code) << "\r\nContent-Length: "
		  << payload.length() << "\r\n"
		  <<  "Content-type: application/json\r\n\r\n" << payload;
}

/**
 * Queue a request to be executed by the execution threads of the dispatcher service
 *
 * @param request	The request to queue
 * @return bool		True if the request wa successfully queued
 */
bool DispatcherApi::queueRequest(ControlRequest *request)
{
	return m_service->queue(request);
}
