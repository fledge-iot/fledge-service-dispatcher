/*
 * Fledge dispatcher service class
 *
 * Copyright (c) 2021 Dianomic Systems
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Massimiliano Pinto
 */
#include <management_api.h>
#include <management_client.h>
#include <service_record.h>
#include <plugin_manager.h>
#include <plugin_api.h>
#include <plugin.h>
#include <logger.h>
#include <iostream>
#include <string>

#include <storage_client.h>
#include <config_handler.h>
#include <dispatcher_service.h>

using namespace std;

/**
 * Constructor for the DispatcherService class
 *
 * This class handles all Dispatcher server components.
 *
 * @param    myName	The Dispatcher server name
 */
DispatcherService::DispatcherService(const string& myName, const string& token) :
					 m_name(myName),
					 m_shutdown(false),
					 m_token(token)
{
	// Default to a dynamic port
	unsigned short servicePort = 0;

	// Create new logger instance
	m_logger = new Logger(myName);
	m_logger->setMinLevel("warning");

	// One thread
	unsigned int threads = 1;

	// Instantiate the DispatcherApi class
	m_api = new DispatcherApi(servicePort, threads);

	// Set NULL for other resources
	m_managerClient = NULL;
	m_managementApi = NULL;
}

/**
 * DispatcherService destructor
 */
DispatcherService::~DispatcherService()
{
	delete m_api;
	delete m_managerClient;
	delete m_managementApi;
	delete m_logger;
}

/**
 * Start the dispactcher service
 * by connecting to Fledge core service.
 *
 * @param coreAddress	The Fledge core address
 * @param corePort	The Fledge core port
 * @return		True for success, false otherwise
 */
bool DispatcherService::start(string& coreAddress,
				unsigned short corePort)
{
	// Dynamic port
	unsigned short managementPort = (unsigned short)0;

	m_logger->info("Starting dispatcher service '" + m_name +  "' ...");

	// Instantiate ManagementApi class
	m_managementApi = new ManagementApi(SERVICE_NAME, managementPort);
	m_managementApi->registerService(this);
	m_managementApi->start();

	// Allow time for the listeners to start before we register
	while(m_managementApi->getListenerPort() == 0)
	{
		sleep(1);
	}

        // Enable http API methods
        m_api->initResources();

        // Start the NotificationApi on service port
	m_api->start();

	// Allow time for the listeners to start before we continue
	while(m_api->getListenerPort() == 0)
	{
		sleep(1);
	}

	// Get management client
	m_managerClient = new ManagementClient(coreAddress, corePort);
	if (!m_managerClient)
	{
		m_logger->fatal("Dispacther service '" + m_name + \
				"' can not connect to Fledge at " + \
				string(coreAddress + ":" + to_string(corePort)));

		this->cleanupResources();
		return false;
	}

	// Create a category with Dispatcher name
	DefaultConfigCategory dispatcherServerConfig(DISPATCHER_CATEGORY, string("{}"));
	dispatcherServerConfig.setDescription("Dispatcher server " + string(DISPATCHER_CATEGORY));
	if (!m_managerClient->addCategory(dispatcherServerConfig, true))
	{
		m_logger->fatal("Dispatcher service '" + m_name + \
				"' can not connect to Fledge ConfigurationManager at " + \
				string(coreAddress + ":" + to_string(corePort)));

		this->cleanupResources();
		return false;
	}

	// Deal with registering and fetching the advanced configuration
	string advancedCatName = DISPATCHER_CATEGORY+string("Advanced");
	DefaultConfigCategory defConfigAdvanced(advancedCatName, string("{}"));
	//addConfigDefaults(defConfigAdvanced);
	defConfigAdvanced.setDescription(DISPATCHER_CATEGORY+string(" advanced config params"));
	vector<string>  logLevels = { "error", "warning", "info", "debug" };
	defConfigAdvanced.addItem("logLevel", "Minimum logging level reported",
                        "warning", "warning", logLevels);
	defConfigAdvanced.setItemDisplayName("logLevel", "Minimum Log Level");

	defConfigAdvanced.addItem("dispatcherThreads",
					 "Maximum number of dispatcher threads",
					 "integer", "2", "2");
	defConfigAdvanced.setItemDisplayName("dispatcherThreads",
						    "Maximun number of dispatcher threads");

	// Create/Update category name (we pass keep_original_items=true)
	if (m_managerClient->addCategory(defConfigAdvanced, true))
	{
		vector<string> children1;
		children1.push_back(advancedCatName);
		m_managerClient->addChildCategories(DISPATCHER_CATEGORY, children1);
	}

	// Register this dispatcher service with Fledge core
	unsigned short listenerPort = m_api->getListenerPort();
	unsigned short managementListener = m_managementApi->getListenerPort();
	ServiceRecord record(m_name,			// Service name
			     "Dispatcher",		// Service type
			     "http",			// Protocol
			     "localhost",		// Listening address
			     listenerPort,		// Service port
			     managementListener,	// Management port
			     m_token);			// Token

	if (!m_managerClient->registerService(record))
	{
		m_logger->fatal("Unable to register service "
				"\"Dispatcher\" for service '" + m_name + "'");

		this->cleanupResources();
		return false;
	}

	// Register DISPATCHER_CATEGORY to Fledge Core
	registerCategory(DISPATCHER_CATEGORY);
	registerCategory(advancedCatName);

	ConfigCategory category = m_managerClient->getCategory(advancedCatName);
	if (category.itemExists("logLevel"))
	{
		m_logger->setMinLevel(category.getValue("logLevel"));
	}

	if (category.itemExists("dispatcherThreads"))
	{
		long val = atol(category.getValue("dispatcherThreads").c_str());
		if (val <= 0)
		{
			m_worker_threads = DEFAULT_WORKER_THREADS;
		}
		else
		{
			m_worker_threads = val;
		}
	}

	// Get Storage service
	ServiceRecord storageInfo("", "Storage");
	if (!m_managerClient->getService(storageInfo))
	{
		m_logger->fatal("Unable to find Fledge storage "
				"connection info for service '" + m_name + "'");

		this->cleanupResources();

		// Unregister from Fledge
		m_managerClient->unregisterService();

		return false;
	}
	m_logger->info("Connecting to storage on %s:%d",
		       storageInfo.getAddress().c_str(),
		       storageInfo.getPort());

	// Setup StorageClient
	StorageClient storageClient(storageInfo.getAddress(),
				    storageInfo.getPort());
	m_storage = &storageClient;

	m_managerClient->addAuditEntry("DSPST",
					"INFORMATION",
					"{\"name\": \"" + m_name + "\"}");

	// .... wait until shutdown ...

	// Wait for all the API threads to complete
	m_api->wait();

	// Shutdown is starting ...
	// NOTE:
	// - Dispatcher API listener is already down.
	// - all xyz already unregistered

	// Unregister from storage service
	m_managerClient->unregisterService();

	// Stop management API
	m_managementApi->stop();

	// Flush all data in the queues

	m_logger->info("Dispatcher service '" + m_name + "' shutdown completed.");

	m_managerClient->addAuditEntry("DSPSD",
					"INFORMATION",
					"{\"name\": \"" + m_name + "\"}");

	return true;
}

/**
 * Unregister dispatcher xyz and
 * stop DispatcherAPi listener
 */
void DispatcherService::stop()
{
	// Stop the DispatcherApi
	m_api->stop();
}

/**
 * Shutdown request
 */
void DispatcherService::shutdown()
{
	m_shutdown = true;
	m_logger->info("Dispatcher service '" + m_name + "' shutdown in progress ...");

	this->stop();
}

/**
 * Cleanup resources and stop services
 */
void DispatcherService::cleanupResources()
{
	this->stop();
	m_api->wait();

	m_managementApi->stop();
}

/**
 * Configuration change
 *
 * @param    categoryName	The category name which configuration has been changed
 * @param    category		The JSON string with new configuration
 */
void DispatcherService::configChange(const string& categoryName,
				       const string& category)
{
	if (categoryName.compare(DISPATCHER_CATEGORY) == 0)
	{
			m_logger->warn("Configuration change for '%s' category not implemented yet",
					DISPATCHER_CATEGORY);
	}

	if (categoryName.compare(string(DISPATCHER_CATEGORY)+"Advanced") == 0)
	{
		ConfigCategory config(categoryName, category);
		if (config.itemExists("logLevel"))
		{
			m_logger->setMinLevel(config.getValue("logLevel"));
			m_logger->info("Setting log level to %s", config.getValue("logLevel").c_str());
		}
	}
	return;
}

/**
 * Register a configuration category for updates
 *
 * @param    categoryName	The category to register
 */
void DispatcherService::registerCategory(const string& categoryName)
{
	ConfigHandler* configHandler = ConfigHandler::getInstance(m_managerClient);
	// Call registerCategory only once
	if (configHandler &&
	    m_registerCategories.find(categoryName) == m_registerCategories.end())
	{
		configHandler->registerCategory(this, categoryName);
		m_registerCategories[categoryName] = true;
	}
}
