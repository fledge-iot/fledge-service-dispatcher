/*
 * Fledge Dispatcher service.
 *
 * Copyright (c) 2021 Dianomic Systems
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Massimiliano Pinto
 */
#include <dispatcher_service.h>
#include <management_api.h>
#include <management_client.h>
#include <service_record.h>
#include <plugin_manager.h>
#include <plugin_api.h>
#include <plugin.h>
#include <logger.h>
#include <iostream>
#include <string>
#include <csignal>
#include <sys/prctl.h>
#include <syslog.h>
#include <execinfo.h>
#include <dlfcn.h>
#include <cxxabi.h>   // for __cxa_demangle

extern int makeDaemon(void);
extern void handler(int sig);

using namespace std;

volatile sig_atomic_t signalReceived = 0;
static DispatcherService *service = NULL;

/**
 * Handle received signals
 *
 * @param    signal	The received signal
 */
static void signalHandler(int signal)
{
	signalReceived = signal;
	if (service)
	{
		// Call stop() method in dispatcher service class
		service->stop();
	}
}

/**
 * Dispatcher service main entry point
 */
int main(int argc, char *argv[])
{
	unsigned short corePort = 8083;
	string	       coreAddress = "localhost";
	bool	       daemonMode = true;
	string	       myName = SERVICE_NAME;
	string	       logLevel = "warning";
	string	       token = "";
	bool	       dryRun = false;

	signal(SIGSEGV, handler);
	signal(SIGILL, handler);
	signal(SIGBUS, handler);
	signal(SIGFPE, handler);
	signal(SIGABRT, handler);

	for (int i = 1; i < argc; i++)
	{
		if (!strcmp(argv[i], "-d"))
		{
			daemonMode = false;
		}
		else if (!strncmp(argv[i], "--port=", 7))
		{
			corePort = (unsigned short)atoi(&argv[i][7]);
		}
		else if (!strncmp(argv[i], "--name=", 7))
		{
			myName = &argv[i][7];
		}
		else if (!strncmp(argv[i], "--address=", 10))
		{
			coreAddress = &argv[i][10];
		}
		else if (!strncmp(argv[i], "--logLevel=", 11))
		{
			logLevel = &argv[i][11];
		}
		else if (!strncmp(argv[i], "--token=", 8))
		{
			token = &argv[i][8];
		}
		else if (!strncmp(argv[i], "--dryrun", 8))
		{
			dryRun = true;
		}
	}

	if (daemonMode && makeDaemon() == -1)
	{
		// Failed to run in daemon mode
		cout << "Failed to run as deamon - "
			"proceeding in interactive mode." << endl;
	}

	// We handle these signals, add more if needed
	std::signal(SIGHUP,  signalHandler);
	std::signal(SIGINT,  signalHandler);
	std::signal(SIGSTOP, signalHandler);
	std::signal(SIGTERM, signalHandler);

	// Instantiate the DispatcherService class
	service = new DispatcherService(myName, token);
	Logger::getLogger()->setMinLevel(logLevel);

	if (dryRun)
	{
		service->setDryRun();
	}

	// Start the Dispatcher service
	service->start(coreAddress, corePort);

	// ... Dispatcher service runs until shutdown ...

	// Service has been stopped
	delete service;
	service = NULL;

	// Return success
	return 0;
}

/**
 * Detach the process from the terminal and run in the background.
 *
 * @return	-1 in case of errors and 0 on success.
 */
int makeDaemon()
{
	pid_t pid;

	int logmask = setlogmask(0);
	/* create new process */
	if ((pid = fork()  ) == -1)
	{
		return -1;  
	}
	else if (pid != 0)  
	{
		exit (EXIT_SUCCESS);  
	}

	// If we got here we are a child process

	// create new session and process group 
	if (setsid() == -1)  
	{
		return -1;  
	}
	setlogmask(logmask);

	// Close stdin, stdout and stderr
	close(0);
	close(1);
	close(2);
	// redirect fd's 0,1,2 to /dev/null
	(void)open("/dev/null", O_RDWR);    // stdin
	(void)dup(0);  			    // stdout	GCC bug 66425 produces warning
	(void)dup(0);  			    // stderr	GCC bug 66425 produces warning

 	return 0;
}

/**
 * Handler to print a stack trace to the syslog
 *
 * @param sig The signal that caused the problem
 */
void handler(int sig)
{
Logger	*logger = Logger::getLogger();
void	*array[20];
char	buf[1024];
int	size;

	// get void*'s for all entries on the stack
	size = backtrace(array, 20);

	// print out all the frames to stderr
	logger->fatal("Signal %d (%s) trapped:\n", sig, strsignal(sig));
	char **messages = backtrace_symbols(array, size);
	for (int i = 0; i < size; i++)
	{
		Dl_info info;
		if (dladdr(array[i], &info) && info.dli_sname)
		{
		    char *demangled = NULL;
		    int status = -1;
		    if (info.dli_sname[0] == '_')
		        demangled = abi::__cxa_demangle(info.dli_sname, NULL, 0, &status);
		    snprintf(buf, sizeof(buf), "%-3d %*p %s + %zd---------",
		             i, int(2 + sizeof(void*) * 2), array[i],
		             status == 0 ? demangled :
		             info.dli_sname == 0 ? messages[i] : info.dli_sname,
		             (char *)array[i] - (char *)info.dli_saddr);
		    free(demangled);
		} 
		else
		{
		    snprintf(buf, sizeof(buf), "%-3d %*p %s---------",
		             i, int(2 + sizeof(void*) * 2), array[i], messages[i]);
		}
		logger->fatal("(%d) %s", i, buf);
	}
	free(messages);
	exit(1);
}

