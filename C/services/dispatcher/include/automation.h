#ifndef _AUTOMATION_H
#define _AUTOMATION_H
/*
 * Fledge Dispatcher service.
 *
 * Copyright (c) 2021 Dianomic Systems
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Mark Riddoch
 *
 * A set of classes that implement the automation scripts
 * that can be used by the control dispatcher
 */
#include <string>
#include <vector>
#include <map>
#include <chrono>
#include <thread>
#include <kvlist.h>

class DispatcherService;
class ScriptStep;

/**
 * A class that represents the script that is executed
 */
class Script {
	public:
		Script(const std::string& name) : m_name(name), m_loaded(false)
		{
		};
		~Script();
		bool		execute(DispatchService *);

	private:
		bool		addStep(int, ScriptStep *);

	private:
		const std::string	m_name;
		bool			m_loaded;
		std::map<int, ScriptStep *>
					m_steps;
};

/**
 * Abstrratc scrpt step class
 */
class ScriptStep {
	public:
		virtual bool execute(DispatcherService *) = 0;
};

/**
 * A script step class for steps that implement a wrote operation on the south service
 */
class WriteScriptStep : public ScriptStep {
	public:
		WriteScriptStep(const std::string& service, const KVList& values) :
						m_service(service), m_values(values)
				{
				};
		bool		execute(DispatcherService *);
	private:
		const std::string	m_service;
		const KVList		m_values;
};

/**
 * A script step for steps that implementation a control operation on a south service
 */
class OperationScriptStep : public ScriptStep {
	public:
		OperationScriptStep(const std::string& operation, const KVList& parameters) :
					m_operation(operation), m_parameters(parameters)
				{
				};
		bool		execute(DispatcherService *);
	private:
		const std::string	m_operation;
		const KVList		m_parameters;
};

/**
 * A script step for a step that causes the automation script to delay
 */
class DelayScriptStep : public ScriptStep {
	public:
		WriteScriptStep(const unsigned int delay) : m_delay(delay)
				{
				};
		bool		execute(DispatcherService *)
				{
					std::this_thread::sleep_for(
							std::chrono::milliseconds(m_delay));
				}
	private:
		unsigned int		delay;
};

/**
 * A script step that changes the configuration of another item within the Fledge
 * instance.
 */
class ConfigScriptStep : public ScriptStep {
	public:
		ConfigScriptStep(const std::string& category, const std::string& name,
					const std::string& value) : m_category(category),
						m_name(name), m_value(value)
				{
				};
		bool		execute(DispatcherService *);
	private:
		const std::string&	m_category;
		const std::string&	m_name;
		const std::string&	m_value;
};

/**
 * A script step that causes another script to be executed. This allows one script
 * to call another.
 */
class ScriptScriptStep : public ScriptStep {
	public:
		ScriptScriptStep(const std::string& name) : m_name(name)
				{
				};
		bool		execute(DispatcherService *);
	private:
		const std::string	m_name;
};

#endif
