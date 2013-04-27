# lshash/storage.py
# Copyright 2012 Kay Zhu (a.k.a He Zhu) and contributors (see CONTRIBUTORS.txt)
#
# This module is part of lshash and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import json

try:
    import redis
except ImportError:
    redis = None

try:
    import pymongo
except ImportError:
    pymongo =None


__all__ = ['storage']


def storage(storage_config, index):
    """ Given the configuration for storage and the index, return the
    configured storage instance.
    """
    if 'dict' in storage_config:
        return InMemoryStorage(storage_config['dict'])
    elif 'redis' in storage_config:
        storage_config['redis']['db'] = index
        return RedisStorage(storage_config['redis'])
    elif 'mongodb' in storage_config:
        storage_config['mongodb']['table_index'] = index
        return MongoStorage(storage_config['mongodb'])
    else:
        raise ValueError("Only in-memory dictionary and Redis are supported.")


class BaseStorage(object):
    def __init__(self, config):
        """ An abstract class used as an adapter for storages. """
        raise NotImplementedError

    def keys(self):
        """ Returns a list of binary hashes that are used as dict keys. """
        raise NotImplementedError

    def set_val(self, key, val):
        """ Set `val` at `key`, note that the `val` must be a string. """
        raise NotImplementedError

    def get_val(self, key):
        """ Return `val` at `key`, note that the `val` must be a string. """
        raise NotImplementedError

    def append_val(self, key, val):
        """ Append `val` to the list stored at `key`.

        If the key is not yet present in storage, create a list with `val` at
        `key`.
        """
        raise NotImplementedError

    def get_list(self, key):
        """ Returns a list stored in storage at `key`.

        This method should return a list of values stored at `key`. `[]` should
        be returned if the list is empty or if `key` is not present in storage.
        """
        raise NotImplementedError


class InMemoryStorage(BaseStorage):
    def __init__(self, config):
        self.name = 'dict'
        self.storage = dict()

    def keys(self):
        return self.storage.keys()

    def set_val(self, key, val):
        self.storage[key] = val

    def get_val(self, key):
        return self.storage[key]

    def append_val(self, key, val):
        self.storage.setdefault(key, []).append(val)

    def get_list(self, key):
        return self.storage.get(key, [])


class MongoStorage(BaseStorage):
    def __init__(self, config):
        # config['db_uri'] = "mongodb://127.0.0.1:27017"
        # config['db_name'] = "lshdb"
        self.client = pymongo.MongoClient(config['db_uri'])
        self.index = config['table_index']
        self.collection_name = config['collection_prefix'] + str(self.index)
        self.db = self.client[config['db_name']]
        self.collection = self.db.get_collection(self.collection_name)

    def keys(self):
        key_list = set()
        for document in self.collection.find():
            key_list.append(document[u'hash_code'])
        return list(key_list)

    def set_val(self, key, val):
        document = {}
        value_list = []
        value_list.append(val)
        document['hash_code'] = key
        document['value'] = value_list
        status_code = self.collection.insert_one(document)
        return status_code.raw_result

    def get_val(self, key):
        query = {}
        query[u'hash_code'] = key
        document = self.collection.find_one(query)
        return document[u'value']

    def append_val(self, key, val):
        query = {}
        query[u'hash_code'] = key
        document = self.collection.find_one(query)
        if document is None:
            document = {}
            value_list = []
            value_list.append(val)
            document['hash_code'] = key
            document['value'] = value_list
            status_code = self.collection.insert_one(document)
        else:
            document[u'value'].append(val)
            status_code = self.collection.update_one(
                {
                    "hash_code": key
                },
                {
                    "$set": {"value": document[u'value']},
                    # "$currentDate": {"lastModified": True}
                }
            )
        # status_code.raw_result
        return status_code

    def get_list(self, key):
        query = {}
        query[u'hash_code'] = key
        document = self.collection.find_one(query)

        result_list = []
        for ele in document[u'value']:
            # ele_tuple = tuple(ele)
            ele_tuple = (tuple(ele[0]),ele[1])
            result_list.append(ele_tuple)

        return tuple(result_list)

    def clean(self):
        return self.collection.drop()


class RedisStorage(BaseStorage):
    def __init__(self, config):
        if not redis:
            raise ImportError("redis-py is required to use Redis as storage.")
        self.name = 'redis'
        self.storage = redis.StrictRedis(**config)

    def keys(self, pattern="*"):
        return self.storage.keys(pattern)

    def set_val(self, key, val):
        self.storage.set(key, val)

    def get_val(self, key):
        return self.storage.get(key)

    def append_val(self, key, val):
        self.storage.rpush(key, json.dumps(val))

    def get_list(self, key):
        return self.storage.lrange(key, 0, -1)
