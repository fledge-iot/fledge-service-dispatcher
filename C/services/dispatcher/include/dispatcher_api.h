#ifndef _DISPATCHER_API_H
#define _DISPATCHER_API_H
/*
 * Fledge Dispatcher service.
 *
 * Copyright (c) 2021 Massimiliano Pinto
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Massimiliano Pinto
 */

#include "logger.h"
#include <server_http.hpp>

using namespace std;
using HttpServer = SimpleWeb::Server<SimpleWeb::HTTP>;

/*
 * URL for each API entry point
 */
#define	DISPATCH_WRITE			"/dispatch/write"
#define DISPATCH_OPERATION		"/dispatch/operation"

#define ESCAPE_SPECIAL_CHARS		"\\{\\}\\\"\\(\\)\\!\\[\\]\\^\\$\\.\\|\\?\\*\\+\\-"

class ControlRequest;
class DispatcherService;

/**
 * DispatcherApi is the entry point for:
 * - Service API
 * - Administration API
 */
class DispatcherApi
{
	public:
		DispatcherApi(const unsigned short port,
				const unsigned int threads);
		~DispatcherApi();
		static		DispatcherApi *getInstance();
		void		initResources();
		void		start(DispatcherService *service);
		void		startServer();
		void		wait();
		void		stop();
		void		stopServer();
		unsigned short	getListenerPort();
		std::string	decodeName(const std::string& name);
		void		defaultResource(shared_ptr<HttpServer::Response> response,
						shared_ptr<HttpServer::Request> request);
		void		write(shared_ptr<HttpServer::Response> response,
						shared_ptr<HttpServer::Request> request);
		void		operation(shared_ptr<HttpServer::Response> response,
						shared_ptr<HttpServer::Request> request);

	private:
		void		internalError(shared_ptr<HttpServer::Response>,
					      const exception&);
		void		respond(shared_ptr<HttpServer::Response>,
					const string&);
		void		respond(shared_ptr<HttpServer::Response>,
					SimpleWeb::StatusCode,
					const string&);
		bool		queueRequest(ControlRequest *);

	private:
		static DispatcherApi*		m_instance;
		HttpServer*			m_server;
		unsigned short			m_port;
		unsigned int			m_threads;
		thread*				m_thread;
		std::string			m_callBackURL;
		Logger*				m_logger;
		DispatcherService		*m_service;
};

#endif

