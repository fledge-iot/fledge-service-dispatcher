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
#include <string_utils.h>

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
		string escaped = p.second;
		StringEscapeQuotes(escaped);
		payload += "\"" + escaped + "\"";
	}
	payload += " }";
	return payload;
}

/**
 * Substitute values into the list.
 *
 * @param values	A key/value list of parameters to substitute
 */
void KVList::substitute(const KVList& values)
{
	for (auto& value : m_list)
	{
		substitute(value.second, values);
	}
}

/**
 * Substitute parameters within a string
 *
 * @param value	The string to perform substitution on
 * @param values	The values to use for the substitution
 */
void KVList::substitute(string& value, const KVList& values)
{
	string rval;
	size_t dstart, p1 = 0;
	while ((dstart = value.find_first_of("$", p1)) != string::npos)
	{
		rval.append(value.substr(p1, dstart - p1));
		dstart++;
		size_t dend = value.find_first_of ("$", dstart);
		if (dend != string::npos)
		{
			string var = value.substr(dstart, dend - dstart);
			rval.append(values.getValue(var));
		}
		else
		{
			Logger::getLogger()->error("Unterminated macro substitution in '%s':%ld", value.c_str(), p1);
		}
		p1 = dend + 1;
	}
	rval.append(value.substr(p1));

	Logger::getLogger()->debug("'%s'", value.c_str());
	Logger::getLogger()->debug("became '%s'", rval.c_str());
	value = rval;
}

/**
 * Construct a reading from a key/value list
 *
 * @param asset		The asset name to use in the reading
 * @return Rading*	A pointer to a newly created reading
 */
Reading *KVList::toReading(const string& asset)
{
vector<Datapoint *> values;

	for (auto& item: m_list)
	{
		// TODO need to do something more sensible with types
		DatapointValue dpv(item.second);
		values.push_back(new Datapoint(item.first, dpv));
	}
	return new Reading(asset, values);
}

/**
 * Replace the content of the Key/Value list with the datapoints in the
 * reading that is passed in. The reading is left in tact upon return,
 * it is the job of the caller to free the reading
 *
 * @param reading	The reading to extract new data from
 */
void KVList::fromReading(Reading *reading)
{
	m_list.clear();
	vector<Datapoint *>datapoints = reading->getReadingData();
	for (Datapoint *dp : datapoints)
	{
		add(dp->getName(), dp->getData().toString());
	}
}
