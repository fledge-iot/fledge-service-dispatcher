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

#define SERVICE_NAME		"Fledge Dispatcher"
#define SERVICE_TYPE		"Dispatcher"
#define DISPATCHER_CATEGORY	"Dispatcher"
#define DEFAULT_WORKER_THREADS 2
/**
 * The DispatcherService class.
 */
class DispatcherService : public ServiceHandler
{
	public:
		DispatcherService(const std::string& name, const std::string& token = "");
		~DispatcherService();
		bool 			start(std::string& coreAddress,
					      unsigned short corePort);
		void 			stop();
		void			shutdown();
		void			cleanupResources();
		void			configChange(const std::string&,
						     const std::string&);
		void			registerCategory(const std::string& categoryName);
		ManagementClient*	getManagementClient() { return m_managerClient; };
		StorageClient*		getStorageClient() { return m_storage; };

	private:
		const std::string	m_name;
		Logger*			m_logger;
		bool			m_shutdown;
		DispatcherApi*		m_api;
		ManagementClient* 	m_managerClient;
		ManagementApi*		m_managementApi;
		StorageClient*		m_storage;
		std::map<std::string, bool>
					m_registerCategories;
		unsigned long		m_worker_threads;
		const std::string       m_token;
};
#endif
