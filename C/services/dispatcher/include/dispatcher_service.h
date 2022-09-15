#ifndef _DISPATCHER_SERVICE_H
#define _DISPATCHER_SERVICE_H
/*
 * Fledge dispatcher service.
 *
 * Copyright (c) 2021 Dianomic Systems
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Massimiliano Pinto
 */

#include <service_handler.h>
#include <management_client.h>
#include <management_api.h>
#include <reading.h>
#include <storage_client.h>
#include <dispatcher_api.h>
#include <controlrequest.h>
#include <mutex>
#include <condition_variable>
#include <queue>

#define SERVICE_NAME		"Fledge Dispatcher"
#define SERVICE_TYPE		"Dispatcher"
#define DEFAULT_WORKER_THREADS	2

/**
 * The DispatcherService class.
 *
 * The main class responsible for managing requests, handling the queues and interfacing
 * to the other Fledge services.
 */
class DispatcherService : public ServiceAuthHandler
{
	public:
		DispatcherService(const std::string& name, const std::string& token = "");
		~DispatcherService();
		bool 			start(std::string& coreAddress,
					      unsigned short corePort);
		void 			stop();
		void			shutdown();
		void			restart();
		bool			isRunning() { return !m_stopping; };
		void			cleanupResources();
		void			configChange(const std::string&,
						     const std::string&);
		void			registerCategory(const std::string& categoryName);
		StorageClient*		getStorageClient() { return m_storage; };
		bool			queue(ControlRequest *request);
		void			worker();
		bool			sendToService(const std::string& service, 
						const std::string& url,
						const std::string& payload,
						const std::string& sourceName,
						const std::string& sourceType);
		void			configChildCreate(const std::string& parent_category,
							const std::string&,
							const std::string&) {};
		void			configChildDelete(const std::string& parent_category,
							const std::string&) {};
		void			setDryRun() { m_dryRun = true; };

	private:
		ControlRequest		*getRequest();

	private:
		Logger*				m_logger;
		bool				m_shutdown;
		DispatcherApi*			m_api;
		ManagementApi*			m_managementApi;
		StorageClient*			m_storage;
		std::map<std::string, bool>
						m_registerCategories;
		unsigned long			m_worker_threads;
		const std::string       	m_token;
		std::queue<ControlRequest *>	m_requests;
		std::mutex			m_mutex;
		std::condition_variable		m_cv;
		bool				m_stopping;
		bool				m_enable;
		bool				m_dryRun;
		bool				m_restartRequest;
};
#endif
