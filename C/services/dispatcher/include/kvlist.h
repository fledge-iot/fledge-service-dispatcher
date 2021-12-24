#ifndef _KVLIST_H
#define _KVLIST_H
/*
 * Fledge Dispatcher service.
 *
 * Copyright (c) 2021 Dianomic Systems
 *
 * Released under the Apache 2.0 Licence
 *
 * Author: Mark Riddoch
 *
 * A common container for key/value pair lists
 */
#include <rapidjson/document.h>
#include <string>
#include <vector>

/**
 * A list of Key/Value pairs encasulated in a class that
 * allows construction from a RapidJSON value. The class
 * also provides a method to serial the list to JSON.
 */
class KVList {
	public:
		KVList() {};
		KVList(rapidjson::Value& value);
		~KVList() {};
		void			add(const std::string& key,
			      	 	    const std::string& value);
		const std::string	getValue(const std::string& key) const;
		std::string		toJSON();
		size_t			size() { return m_list.size(); };

	private:
		std::vector<std::pair<std::string, std::string> >
					m_list;
};
#endif
