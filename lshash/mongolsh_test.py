# -*- coding:utf-8 -*-
from lshash import LSHash


def test1():
    lsh = LSHash(6, 8)
    lsh.index([1, 2, 3, 4, 5, 6, 7, 8], extra_data=1)
    lsh.index([2, 3, 4, 5, 6, 7, 8, 9], extra_data=2)
    lsh.index([10, 12, 99, 1, 5, 31, 2, 3], extra_data=3)
    print lsh.query([1, 2, 3, 4, 5, 6, 7, 7])


# 输出的结果格式
# (((vector), extra_data), distance)
def test_mongodb_storage():
    mongo_config = {}
    mongo_config['db_uri'] = "mongodb://127.0.0.1:27017"
    mongo_config['db_name'] = "lshdb"
    mongo_config['collection_prefix'] = "mongolsh"
    storage_config = {}
    storage_config['mongodb'] = mongo_config
    # storage_config['dict'] = None

    lsh = LSHash(hash_size=32,input_dim=8,storage_config=storage_config,num_hashtables=5)

    lsh.index([1, 2, 3, 4, 5, 6, 7, 8], extra_data="app1")
    lsh.index([1, 2, 3, 4, 5, 6, 7, 7], extra_data="app2")
    lsh.index([2, 3, 4, 5, 6, 7, 8, 9], extra_data="app3")
    lsh.index([10, 12, 99, 1, 5, 31, 2, 3], extra_data="app4")

    print '---------- Query Result ----------'
    print lsh.query([1, 2, 3, 4, 5, 6, 7, 7])
    lsh.clean()


if __name__ == '__main__':
    # test1()
    test_mongodb_storage()
