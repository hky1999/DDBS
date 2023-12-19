import redis
import pymongo
import json
import sys
import os

localhost = '127.0.0.1'
db_generation = os.getcwd() + 'db-generation'

def init_redis(port):
    return redis.Redis(host=localhost, port=port, decode_responses=True)

def init_mongo(port):
    conn = pymongo.MongoClient(host=localhost, port=port)
    return conn['db']

def init_data(cache, dbms):
    user = dbms['user']
    article = dbms['article']

    x = user.insert_one({'name':'A', 'gender': 'B'})

    cache.set('user', json.dumps({'name':'A', 'gender': 'B'}))
    print(cache.get('user'))

if __name__ == "__main__":
    # Machine 0
    cache0 = init_redis(20000)
    dbms0 = init_mongo(20001)
    init_data(cache0, dbms0)

    # Machine 1
    cache1 = init_redis(20002)
    dbms1 = init_mongo(20003)
