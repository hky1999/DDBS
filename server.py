#!/usr/bin/env python3
# encoding: utf-8

import argparse
import redis
import pymongo
import json
import os
import hdfs

import config
from tqdm import tqdm
# from concurrent.futures import ThreadPoolExecutor, as_completed

db_generation = 'db-generation'

def init_redis(host, port):
    return redis.Redis(host=host, port=port, decode_responses=True)

def init_mongo(host, port):
    conn = pymongo.MongoClient(host=host, port=port)
    return conn['db']

def init_hdfs(host, port):
    return hdfs.Client(url=f"http://{host}:{port}")

def init():
    handles = {}
    for name in config.component_names:
        host, port = config.component_addresses[name]
        if name.startswith('cache'):
            handle = init_redis(host, port)
        elif name.startswith('dbms'):
            handle = init_mongo(host, port)
        elif name.startswith('hdfs'):
            handle = init_hdfs(host, port)
        else:
            assert 0
        handles[name] = handle
    return handles


def map_document(doc: dict, mapping: dict):
    return { k: (mapping[k](v) if k in mapping else v) for k, v in doc.items() }

def to_int_transforms(fields: list[str]):
    return { k: int for k in fields }


def init_single_collection(table_name, index_key, db_sites, datafile_path, field_transforms):
    for db in db_sites.values():
        db.drop_collection(table_name)
        db[table_name].create_index(index_key)

    placement = {} # uid -> locations
    shard_key, shard_map = config.sharding_rules[table_name]

    with open(datafile_path) as f:
        lines = f.readlines()
    for line in tqdm(lines):
        item = map_document(json.loads(line), field_transforms)

        value = item[shard_key]
        assert value in shard_map
        locations = shard_map[value]
        placement[item[index_key]] = locations
        for site in locations:
            db_sites[site][table_name].insert_one(item)
    return placement


def init_read_collection(db_sites, datafile_path, user_placement):
    for db in db_sites.values():
        db.drop_collection('read')
        # create indexes
        read = db['read']
        read.create_index('timestamp')
        read.create_index('uid')
        read.create_index('aid')

    field_transforms = to_int_transforms(['timestamp', 'uid', 'aid', 'readTimeLength', 'agreeOrNot', 'commentOrNot', 'shareOrNot'])
    with open(datafile_path) as f:
        lines = f.readlines()
    for line in tqdm(lines):
        item = map_document(json.loads(line), field_transforms)
        locations = user_placement[item['uid']]
        for site in locations:
            db_sites[site]['read'].insert_one(item)


def populate_new_collections(db_sites, article_placement):
    for db in db_sites.values():
        db.drop_collection('be_read')
        db.drop_collection('popular_rank')

        be_read = db['be_read']
        be_read.create_index('timestamp')
        be_read.create_index('aid')

        popular_rank = db['popular_rank']
        popular_rank.create_index('timestamp')
    
    # query min/max timestamp
    min_timestamps, max_timestamps = [], []
    for db in db_sites.values():
        cursor = db['read'].aggregate([
            {
                '$group': {'_id': None, 'min': {'$min': '$timestamp'}, 'max': {'$max': '$timestamp'}}
            }
        ])
        result = next(cursor)
        min_timestamps.append(result['min'])
        max_timestamps.append(result['max'])
    min_timestamp = min(min_timestamps)
    max_timestamp = max(max_timestamps)

    interval_ms = 24 * 60 * 60 * 1000
    min_timestamp = min_timestamp // interval_ms * interval_ms
    max_timestamp = (max_timestamp + interval_ms - 1) // interval_ms * interval_ms

    # populate be-read collection
    select = lambda lst, mask: [x for x, m in zip(lst, mask) if m]
    aggr_fields = ('agree', 'share', 'comment')
    for ts in tqdm(range(min_timestamp, max_timestamp, interval_ms)):
        article_aggregation = {}
        for db in db_sites.values():
            cursor = db['read'].aggregate([
                {
                    '$match': {'timestamp': {'$lt': ts}}
                },
                {
                    '$group': {
                        '_id': '$aid',
                        'readNum': {'$sum': 1},
                        'readUidList': {'$push': '$uid'},
                        'agreeMask': {'$push': '$agreeOrNot'},
                        'shareMask': {'$push': '$shareOrNot'},
                        'commentMask': {'$push': '$commentOrNot'}
                    }
                },
            ])

            # maybe some operations should be performed using MQL
            for item in cursor:
                aid = item['_id']
                for field in aggr_fields:
                    mask = item[field + 'Mask']
                    item[field + 'Num'] = sum(mask)
                    item[field + 'UidList'] = select(item['readUidList'], mask)
                    del item[field + 'Mask']
                del item['_id']
 
                # do simple aggregation since collection `read` is fragmented without replica
                aggr = article_aggregation.get(aid)
                if aggr is None:
                    article_aggregation[aid] = item
                else:
                    for k, v in item.items():
                        aggr[k] = aggr[k] + v

        for aid, item in article_aggregation.items():
            item['aid'] = aid
            item['timestamp'] = ts

            for site in article_placement[aid]:
                db_sites[site]['be_read'].insert_one(item)
    
    # populate popular_rank collection
    top_k = 5
    query_site = 'dbms1' # hard-coded, dbms1 has full `be_read` collection
    be_read_coll = db_sites[query_site]['be_read']
    location_map = config.sharding_rules['popular_rank'][1]
    for ub in tqdm(range(min_timestamp + interval_ms, max_timestamp, interval_ms)):
        for gran_num, gran_str in config.temporal_granularities:
            lb = max(ub - gran_num * interval_ms, min_timestamp)

            ub_results = be_read_coll.find({'timestamp': ub})
            lb_results = be_read_coll.find({'timestamp': lb})

            read_nums = {} # aid -> readNum
            for item in ub_results:
                read_nums[item['aid']] = item['readNum']
            for item in lb_results:
                read_nums[item['aid']] -= item['readNum']
            
            article_info = sorted(list(read_nums.items()), key=lambda t: t[1], reverse=True)
            article_aids = [t[0] for t in article_info[:top_k]]
            article_read_nums = [t[1] for t in article_info[:top_k]]

            item = {
                'timestamp': ub,
                'temporalGranularity': gran_str,
                'articleAidList': article_aids,
                'articleReadNumList': article_read_nums,
            }
            for site in location_map[gran_str]:
                db_sites[site]['popular_rank'].insert_one(item)


def init_database_tables(handles):
    dbs = { name: handle for name, handle in handles.items() if name.startswith('dbms') }
    
    user_transforms = to_int_transforms(['timestamp', 'uid', 'obtainedCredits'])
    article_transforms = to_int_transforms(['timestamp', 'aid'])

    user_placement = init_single_collection('user', 'uid',
                                            dbs, os.path.join(db_generation, 'user.dat'), user_transforms)
    article_placement = init_single_collection('article', 'aid',
                                               dbs, os.path.join(db_generation, 'article.dat'), article_transforms)
    
    init_read_collection(dbs, os.path.join(db_generation, 'read.dat'), user_placement)
    populate_new_collections(dbs, article_placement)


def init_hdfs_content(client: hdfs.Client):
    client.makedirs("/data")
    client.makedirs("/data/image")
    client.makedirs("/data/video")
    client.upload(
        hdfs_path="/data/image",
        local_path=os.path.join(db_generation, "image/"),
        overwrite=True,
        cleanup=True,
        n_threads=8,
        chunk_size=2**20,
    )
    print(client.list("/data/image"))
    client.upload(
        hdfs_path="/data/video",
        local_path=os.path.join(db_generation, "video/"),
        overwrite=True,
        cleanup=True,
    )
    print(client.list("/data/video"))


def query_single_table(handles, table_name, condition, remove_id=True):
    # TODO: add cache support
    results = {}
    for dbms, cache in config.dbms_nodes:
        cursor = handles[dbms][table_name].find(condition)
        for item in cursor:
            obj_id = item['_id']
            if remove_id:
                del item['_id']
            results[obj_id] = item
    return list(results.values())


def get_all_articles(handles):
    return query_single_table(handles, 'article', None, True)


def query_user_read(handles, read_condition):
    # TODO: add cache support
    articles = get_all_articles(handles)

    user_reads = []
    for dbms, cache in config.dbms_nodes:
        handles[dbms].drop_collection('tmp_article')
        tmp_articles = handles[dbms]['tmp_article']
        # tmp_articles.create_index('aid') # might not be very useful here
        tmp_articles.insert_many(articles)

        cursor = handles[dbms]['read'].aggregate([
            {
                '$match': read_condition
            },
            {
                '$lookup': {
                    'from': 'tmp_article',
                    'localField': 'aid',
                    'foreignField': 'aid',
                    'as': 'articles',
                    # 'pipeline': [{'$documents': articles}], # NOTE: extremely slow, don't use
                }
            },
            {
                '$group': {
                    '_id': '$uid',
                    'readList': {
                        '$push': {
                            'id': '$id',
                            'article': {'$arrayElemAt': ['$articles', 0]},
                            # more fields from read object
                        }
                    },
                }
            },
            {
                '$lookup': {
                    'from': 'user',
                    'localField': '_id',
                    'foreignField': 'uid',
                    'as': 'user',
                }
            },
        ])
        for item in cursor:
            user_reads.append(item)
    return user_reads


def query_popular_articles(handles, timestamp, top_k=5):
    # solution 1: query be-read table
    interval_ms = 24 * 60 * 60 * 1000
    # timestamp = timestamp // interval_ms * interval_ms

    def get_max_timestamp_lte(coll, limit):
        max_ts_cursor = coll.aggregate([
            { 
                '$match': {'timestamp': {'$lte': limit}}
            },
            {
                '$group': {
                    '_id': None,
                    'maxTs': {'$max': '$timestamp'},
                }
            },
        ])
        max_ts_result = list(max_ts_cursor)
        if len(max_ts_result) > 0:
            return max_ts_result[0]['maxTs']
        return None

    # TODO: fix this hard-coded dbms1
    be_read_coll = handles['dbms1']['be_read'] # the full be_read collection
    read_nums_ub = {}
    max_ts = get_max_timestamp_lte(be_read_coll, timestamp)
    if max_ts is not None:
        for item in be_read_coll.find({'timestamp': max_ts}):
            read_nums_ub[item['aid']] = item['readNum']

    results = {}
    articles = { item['aid']: item for item in get_all_articles(handles) }
    for gran_val, gran_str in config.temporal_granularities:
        read_nums = { aid: read_num for aid, read_num in read_nums_ub.items() }
        max_ts = get_max_timestamp_lte(be_read_coll, timestamp - interval_ms * gran_val)
        if max_ts is not None:
            for item in be_read_coll.find({'timestamp': max_ts}):
                read_nums[item['aid']] -= item['readNum']
            
        article_info = sorted(read_nums.items(), key=lambda t: t[1], reverse=True)
        article_info = article_info[:top_k]

        results[gran_str] = [
            {'count': count, 'article': articles[aid]} for aid, count in article_info
        ]
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--init', action='store_true', required=False, default=False,
                        help='if set, bulk insert data into databases & hdfs')
    args = parser.parse_args()

    handles = init()

    if args.init:
        init_database_tables(handles)
        init_hdfs_content(handles['hdfs'])

    # from IPython import embed
    # embed()
