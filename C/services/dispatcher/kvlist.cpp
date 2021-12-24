/*
 * Fledge Dispatcher class for key/value lists
 *
 * Copyright (c) 2021 Dianomic Systems
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Mark Riddoch
 */
#include <kvlist.h>
#include <logger.h>
#include <stdexcept>

using namespace std;
using namespace rapidjson;

/**
 * Construct a key/value list from a RapidJSON object
 */
KVList::KVList(Value& value)
{

	if (value.IsObject())
	{
		for (auto& kv : value.GetObject())
		{
			string key = kv.name.GetString();
			if (kv.value.IsString())
			{
				m_list.push_back(pair<string, string>(key, kv.value.GetString()));
			}
			else
			{
				throw runtime_error("Value in key/value pair should be an integer");
			}
		}
	}
	else
	{
		throw runtime_error("Expected JSON value to be an object");
	}
}

/**
 * Add a key/value pair to the list
 *
 * @param key	The key
 * @param value	The value for the key
 */
void KVList::add(const string& key, const string& value)
{
	m_list.push_back(pair<string, string>(key, value));
}

/**
 * Return the value for a given key
 *
 * @param key	The key to lookup
 * @return string	The value or an empty string if the key was not found
 */
const string KVList::getValue(const string& key) const
{
	for (auto &p : m_list)
	{
		if (key.compare(p.first) == 0)
			return p.second;
	}
	return string("");
}

/**
 * Return the key/value pair list as a set of JSON properties
 *
 * @return string	The key/value pair list as JSON
 */
string KVList::toJSON()
{
	string payload = "{ ";
	bool first = true;
	for (auto &p : m_list)
	{
		if (first)
			first = false;
		else
			payload += ", ";
		payload += "\"" + p.first + "\" :";
		payload += "\"" + p.second + "\"";
	}
	payload += " }";
	return payload;
}
