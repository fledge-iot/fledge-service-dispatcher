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
#include <rapidjson/document.h>

class DispatcherService;
class ScriptStep;

#define SCRIPT_TABLE	"control_script"

/**
 * A class that represents the script that is executed
 */
class Script {
	public:
		Script(const std::string& name) : m_name(name), m_loaded(false)
		{
		};
		~Script();
		bool		execute(DispatcherService *, const KVList&);

	private:
		bool		addStep(int, ScriptStep *);
		bool		load(DispatcherService *);
		ScriptStep	*parseStep(const std::string& type, const rapidjson::Value& value);
		bool		addCondition(ScriptStep *step, const rapidjson::Value& value);

	private:
		const std::string	m_name;
		bool			m_loaded;
		std::map<int, ScriptStep *>
					m_steps;
};

/**
 * Abstract script step class
 */
class ScriptStep {
	public:
		virtual bool execute(DispatcherService *, const KVList&) = 0;
		void		addCondition(const std::string& key, const std::string& op,
						const std::string& value)
			        {
					m_key = key;
					m_op = op;
					m_value = value;
				}
	protected:
		bool			evaluate(const KVList& values);

	private:
		std::string		m_key;
		std::string		m_op;
		std::string		m_value;
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
		bool		execute(DispatcherService *, const KVList&);
	private:
		const std::string	m_service;
		KVList			m_values;
};

/**
 * A script step for steps that implementation a control operation on a south service
 */
class OperationScriptStep : public ScriptStep {
	public:
		OperationScriptStep(const std::string& operation, const std::string& service, const KVList& parameters) :
					m_operation(operation), m_service(service), m_parameters(parameters)
				{
				};
		bool		execute(DispatcherService *, const KVList&);
	private:
		const std::string	m_operation;
		const std::string	m_service;
		KVList			m_parameters;
};

/**
 * A script step for a step that causes the automation script to delay
 */
class DelayScriptStep : public ScriptStep {
	public:
		DelayScriptStep(const unsigned int delay) : m_delay(delay)
				{
				};
		bool		execute(DispatcherService *service, const KVList& parameters)
				{
					if (evaluate(parameters))
					{
						std::this_thread::sleep_for(
							std::chrono::milliseconds(m_delay));
					}
				}
	private:
		unsigned int		m_delay;
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
		bool		execute(DispatcherService *, const KVList&);
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
		bool		execute(DispatcherService *, const KVList&);
	private:
		const std::string	m_name;
};

#endif
