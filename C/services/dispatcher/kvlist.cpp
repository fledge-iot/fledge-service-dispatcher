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
				Logger::getLogger()->debug("Parameter: %s is %s", key.c_str(), kv.value.GetString());
				m_list.push_back(pair<string, string>(key, kv.value.GetString()));
			}
			else
			{
				throw runtime_error("Value in key/value pair should be a string");
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
 * Return the key/value pair list as a string
 *
 * @return string	The key/value pair list as a string
 */
string KVList::toString() const
{
	string parameters = "( ";
	bool first = true;
	for (auto &p : m_list)
	{
		if (first)
			first = false;
		else
			parameters += ", ";
		parameters += "\"" + p.first + "\" :";
		string escaped = p.second;
		StringEscapeQuotes(escaped);
		parameters += "\"" + escaped + "\"";
	}
	parameters += " )";
	return parameters;
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
		// TODO need to do something with complex types
		switch (deduceType(item.second))
		{
			case DatapointValue::T_STRING:
			default:
			{
				DatapointValue dpv(item.second);
				values.push_back(new Datapoint(item.first, dpv));
				break;
			}
			case DatapointValue::T_INTEGER:
			{
				long val = strtol(item.second.c_str(), NULL, 10);
				DatapointValue dpv(val);
				values.push_back(new Datapoint(item.first, dpv));
				break;
			}
			case DatapointValue::T_FLOAT:
			{
				double val = strtod(item.second.c_str(), NULL);
				DatapointValue dpv(val);
				values.push_back(new Datapoint(item.first, dpv));
				break;
			}
		}
	}
	// We can not have a reading with no data points, so if we have no parameters
	// we must add a dummy datapoint to pass the operation through the filter
	if (values.size() == 0)
	{
		DatapointValue dpv("None");
		values.push_back(new Datapoint("__None__", dpv));
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
	if (!reading)
		return;
	vector<Datapoint *>datapoints = reading->getReadingData();
	for (Datapoint *dp : datapoints)
	{
		// Remove the dummy datapoint that was added
		if (dp->getName().compare("__None__") != 0)
		{
			try {
				if (dp->getData().getType() == DatapointValue::T_STRING)
					add(dp->getName(), dp->getData().toStringValue());
				else
					add(dp->getName(), dp->getData().toString());
			} catch (exception& e) {
				Logger::getLogger()->warn("Unable to add datapoint %s of type %s returned from pipeline, %s.", dp->getName(), dp->getData().getTypeStr(), e.what());
			}
		}
	}
}

/**
 * Examine the string that is passed in a deduce a suitable type
 * for the datapoint that will be created.
 *
 * @param value	The value to examine
 * @return DatapointTag	The deduced type
 */
DatapointValue::DatapointTag KVList::deduceType(const string& value)
{
DatapointValue::DatapointTag rval = DatapointValue::T_STRING;

	bool numeric = true;
	bool floating = false;

	for (int i = 0; i < value.length(); i++)
	{
		if ((value[i] < '0' || value[i] > '9') && value[i] != '.')
		{
			numeric = false;
			break;
		}
		else if (value[i] == '.' && floating == false)
		{
			floating = true;
			break;
		}
		else if (value[i] == '.' && floating == true)
		{
			floating = false;
			break;
		}
	}
	if (numeric && floating)
		rval = DatapointValue::T_FLOAT;
	else if (numeric)
		rval = DatapointValue::T_INTEGER;
	return rval;
}

