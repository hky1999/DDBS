#!/usr/bin/env python3
# encoding: utf-8

import argparse
import redis
import pymongo
import json
import os
import hdfs
import base64

from tqdm import tqdm
from flask import Flask, request
from bson.objectid import ObjectId

from . import config

db_generation = "db-generation"


class IdCounter:
    def __init__(self):
        self.v = 0

    def incr(self) -> int:
        self.v += 1
        return self.v


def init_redis(host, port):
    return redis.Redis(host=host, port=port, decode_responses=True)


def init_mongo(host, port):
    conn = pymongo.MongoClient(host=host, port=port)
    return conn["db"]


def init_hdfs(host, port):
    return hdfs.Client(url=f"http://{host}:{port}")


def init():
    '''
    Initialize handles for all components: cache, dbms, storage
    Use redis for cache, mongodb for dbms, hdfs for storage
    '''
    handles = {}
    for name in config.component_names:
        host, port = config.component_addresses[name]
        if name.startswith("cache"):
            handle = init_redis(host, port)
        elif name.startswith("dbms"):
            handle = init_mongo(host, port)
        elif name.startswith("hdfs"):
            handle = init_hdfs(host, port)
        else:
            assert 0
        handles[name] = handle
    return handles


def map_document(doc: dict, mapping: dict):
    '''
    Apply mapping to a document.
    If a field is not in the mapping, it is left unchanged.
    '''
    return {k: (mapping[k](v) if k in mapping else v) for k, v in doc.items()}


def to_int_transforms(fields: list[str]):
    '''
    Return a dict that maps field names to int conversion function
    '''
    return {k: int for k in fields}


def init_single_collection(
    table_name, index_key, db_sites, datafile_path, field_transforms
):
    '''
    Shard a collection and populate it with data from a file
    Will be used to initialize user and article collections
    '''
    for db in db_sites.values():
        db.drop_collection(table_name)
        db[table_name].create_index(index_key)

    placement = {}  # uid -> locations
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
    '''
    Populate read collection with data from a file
    '''
    for db in db_sites.values():
        db.drop_collection("read")
        # create indexes
        read = db["read"]
        read.create_index("timestamp")
        read.create_index("uid")
        read.create_index("aid")

    field_transforms = to_int_transforms(
        [
            "timestamp",
            "uid",
            "aid",
            "readTimeLength",
            "agreeOrNot",
            "commentOrNot",
            "shareOrNot",
        ]
    )
    with open(datafile_path) as f:
        lines = f.readlines()
    for line in tqdm(lines):
        item = map_document(json.loads(line), field_transforms)
        locations = user_placement[item["uid"]]
        for site in locations:
            db_sites[site]["read"].insert_one(item)


def populate_new_collections(db_sites, article_placement):
    '''
    Populate be-read and popular_rank collections based on read collection
    '''
    for db in db_sites.values():
        db.drop_collection("be_read")
        db.drop_collection("popular_rank")

        be_read = db["be_read"]
        be_read.create_index("timestamp")
        be_read.create_index("aid")

        popular_rank = db["popular_rank"]
        popular_rank.create_index("timestamp")

    # query min/max timestamp
    min_timestamps, max_timestamps = [], []
    for db in db_sites.values():
        cursor = db["read"].aggregate(
            [
                {
                    "$group": {
                        "_id": None,
                        "min": {"$min": "$timestamp"},
                        "max": {"$max": "$timestamp"},
                    }
                }
            ]
        )
        result = next(cursor)
        min_timestamps.append(result["min"])
        max_timestamps.append(result["max"])
    min_timestamp = min(min_timestamps)
    max_timestamp = max(max_timestamps)

    interval_ms = 24 * 60 * 60 * 1000
    min_timestamp = min_timestamp // interval_ms * interval_ms
    max_timestamp = (max_timestamp + interval_ms - 1) // interval_ms * interval_ms

    # populate be-read collection
    be_read_id = IdCounter()
    select = lambda lst, mask: [x for x, m in zip(lst, mask) if m]
    aggr_fields = ("agree", "share", "comment")
    for ts in tqdm(range(min_timestamp, max_timestamp, interval_ms)):
        article_aggregation = {}
        for db in db_sites.values():
            cursor = db["read"].aggregate(
                [
                    {"$match": {"timestamp": {"$lt": ts}}},
                    {
                        "$group": {
                            "_id": "$aid",
                            "readNum": {"$sum": 1},
                            "readUidList": {"$push": "$uid"},
                            "agreeMask": {"$push": "$agreeOrNot"},
                            "shareMask": {"$push": "$shareOrNot"},
                            "commentMask": {"$push": "$commentOrNot"},
                        }
                    },
                ]
            )

            # maybe some operations should be performed using MQL
            for item in cursor:
                aid = item["_id"]
                for field in aggr_fields:
                    mask = item[field + "Mask"]
                    item[field + "Num"] = sum(mask)
                    item[field + "UidList"] = select(item["readUidList"], mask)
                    del item[field + "Mask"]
                del item["_id"]

                # do simple aggregation since collection `read` is fragmented without replica
                aggr = article_aggregation.get(aid)
                if aggr is None:
                    article_aggregation[aid] = item
                else:
                    for k, v in item.items():
                        aggr[k] = aggr[k] + v

        for aid, item in article_aggregation.items():
            item["id"] = f"br{be_read_id.incr()}"
            # semantically, (aid, timestamp) is unique
            item["aid"] = aid
            item["timestamp"] = ts

            for site in article_placement[aid]:
                db_sites[site]["be_read"].insert_one(item)

    # populate popular_rank collection
    popular_rank_id = IdCounter()
    top_k = 5
    query_site = "dbms1"  # hard-coded, dbms1 has full `be_read` collection
    be_read_coll = db_sites[query_site]["be_read"]
    location_map = config.sharding_rules["popular_rank"][1]
    for ub in tqdm(range(min_timestamp + interval_ms, max_timestamp, interval_ms)):
        for gran_num, gran_str in config.temporal_granularities:
            lb = max(ub - gran_num * interval_ms, min_timestamp)

            ub_results = be_read_coll.find({"timestamp": ub})
            lb_results = be_read_coll.find({"timestamp": lb})

            read_nums = {}  # aid -> readNum
            for item in ub_results:
                read_nums[item["aid"]] = item["readNum"]
            for item in lb_results:
                read_nums[item["aid"]] -= item["readNum"]

            article_info = sorted(
                list(read_nums.items()), key=lambda t: t[1], reverse=True
            )
            article_aids = [t[0] for t in article_info[:top_k]]
            article_read_nums = [t[1] for t in article_info[:top_k]]

            item = {
                "id": f"pr{popular_rank_id.incr()}",
                "timestamp": ub,
                "temporalGranularity": gran_str,
                "articleAidList": article_aids,
                "articleReadNumList": article_read_nums,
            }
            for site in location_map[gran_str]:
                db_sites[site]["popular_rank"].insert_one(item)


def init_database_tables(handles):
    '''
    Initialize database tables for dbms on different sites with data from json files
    
    init_single_collection: user, article
    init_read_collection(user): read
    populate_new_collections(article): be_read, popular_rank
    '''
    dbs = {name: handle for name, handle in handles.items() if name.startswith("dbms")}

    user_transforms = to_int_transforms(["timestamp", "uid", "obtainedCredits"])
    article_transforms = to_int_transforms(["timestamp", "aid"])

    user_placement = init_single_collection(
        "user", "uid", dbs, os.path.join(db_generation, "user.dat"), user_transforms
    )
    article_placement = init_single_collection(
        "article",
        "aid",
        dbs,
        os.path.join(db_generation, "article.dat"),
        article_transforms,
    )

    init_read_collection(dbs, os.path.join(db_generation, "read.dat"), user_placement)
    populate_new_collections(dbs, article_placement)


def init_hdfs_content(client: hdfs.Client):
    hdfs_path = "/data/articles"
    local_path = os.path.join(db_generation, "articles/")
    client.makedirs(hdfs_path)

    total_files = sum(len(files) for path, dirs, files in os.walk(local_path))
    with tqdm(range(total_files)) as t:

        def progress(path, n):
            if n == -1:
                t.update(1)

        client.upload(
            hdfs_path=hdfs_path,
            local_path=local_path,
            overwrite=True,
            cleanup=True,
            n_threads=16,
            progress=progress,
            chunk_size=2**20,
        )


def query_single_table_direct(handles, table_name, condition=None, id_key="_id"):
    # NOTE: bypass redis, query mongodb directly
    results = {}
    for dbms, cache in config.dbms_nodes:
        cursor = handles[dbms][table_name].find(condition)
        for item in cursor:
            item["_id"] = str(item["_id"])
            results[item[id_key]] = item
    return list(results.values())


def query_single_table_cached(handles, table_name, condition=None, id_key="_id"):
    # cache is not used when doing full table retrieval
    if condition is None:
        return query_single_table_direct(handles, table_name, condition, id_key)

    assert isinstance(condition, (int, str, dict))
    # int/str: get by id, dict: complex query
    if not isinstance(condition, dict):  # id query
        assert id_key != "_id"  # TODO: raise exception instead
        redis_hash_key = table_name
        redis_hash_field = str(condition)
        condition = {id_key: condition}
    else:  # complex query
        redis_hash_key = table_name + "_query"
        redis_hash_field = json.dumps(condition)

    results = {}
    for dbms, cache in config.dbms_nodes:
        redis = handles[cache]
        cached = redis.hget(redis_hash_key, redis_hash_field)

        if cached is not None:
            local_items = json.loads(cached)
        else:
            local_items = []
            for item in handles[dbms][table_name].find(condition):
                item["_id"] = str(item["_id"])
                local_items.append(item)
            if len(local_items) > 0:
                redis.hset(redis_hash_key, redis_hash_field, json.dumps(local_items))

        for item in local_items:
            results[item[id_key]] = item
    return list(results.values())


def get_all_articles(handles):
    return query_single_table_direct(handles, "article", None, "aid")


def query_user_read(handles, read_condition):
    # NOTE: 涉及多表join，暂时没走缓存
    # 由于article跟read的分表方式不一致，所以先获取了全部article作为临时表参与lookup
    # 而user与read分表方式一致，所以可以做本地lookup
    # 且因为划分无重叠，最终没有做按id合并的步骤
    articles = get_all_articles(handles)

    user_reads = []
    for dbms, cache in config.dbms_nodes:
        handles[dbms].drop_collection("tmp_article")
        tmp_articles = handles[dbms]["tmp_article"]
        # tmp_articles.create_index('aid') # might not be very useful here
        tmp_articles.insert_many(articles)

        cursor = handles[dbms]["read"].aggregate(
            [
                {"$match": read_condition},
                {
                    "$lookup": {
                        "from": "tmp_article",
                        "localField": "aid",
                        "foreignField": "aid",
                        "as": "articles",
                        # 'pipeline': [{'$documents': articles}], # NOTE: extremely slow, don't use
                    }
                },
                {
                    "$group": {
                        "_id": "$uid",
                        "readList": {
                            "$push": {
                                "id": "$id",
                                "article": {"$arrayElemAt": ["$articles", 0]},
                                # more fields from read object
                            }
                        },
                    }
                },
                {
                    "$lookup": {
                        "from": "user",
                        "localField": "_id",
                        "foreignField": "uid",
                        "as": "user",
                    }
                },
            ]
        )
        for item in cursor:
            user_reads.append(item)
    return user_reads


def query_popular_articles(handles, timestamp, top_k=5):
    # solution 1: query be-read table
    interval_ms = 24 * 60 * 60 * 1000
    # timestamp = timestamp // interval_ms * interval_ms

    def get_max_timestamp_lte(coll, limit):
        max_ts_cursor = coll.aggregate(
            [
                {"$match": {"timestamp": {"$lte": limit}}},
                {
                    "$group": {
                        "_id": None,
                        "maxTs": {"$max": "$timestamp"},
                    }
                },
            ]
        )
        max_ts_result = list(max_ts_cursor)
        if len(max_ts_result) > 0:
            return max_ts_result[0]["maxTs"]
        return None

    # TODO: fix this hard-coded dbms1
    be_read_coll = handles["dbms1"]["be_read"]  # the full be_read collection
    read_nums_ub = {}
    max_ts = get_max_timestamp_lte(be_read_coll, timestamp)
    if max_ts is not None:
        for item in be_read_coll.find({"timestamp": max_ts}):
            read_nums_ub[item["aid"]] = item["readNum"]

    results = {}
    articles = {item["aid"]: item for item in get_all_articles(handles)}
    for gran_val, gran_str in config.temporal_granularities:
        read_nums = {aid: read_num for aid, read_num in read_nums_ub.items()}
        max_ts = get_max_timestamp_lte(be_read_coll, timestamp - interval_ms * gran_val)
        if max_ts is not None:
            for item in be_read_coll.find({"timestamp": max_ts}):
                read_nums[item["aid"]] -= item["readNum"]

        article_info = sorted(read_nums.items(), key=lambda t: t[1], reverse=True)
        article_info = article_info[:top_k]

        results[gran_str] = [
            {"count": count, "article": articles[aid]} for aid, count in article_info
        ]
    return results


def fetch_article_details(hdfs: hdfs.Client, article):
    details = {}
    fields = ("text", "image", "video")
    aid = article["aid"]
    path_prefix = f"/data/articles/article{aid}/"
    for field in fields:
        for filename in article[field].split(","):
            if len(filename) == 0:
                continue
            with hdfs.read(os.path.join(path_prefix, filename)) as reader:
                content = reader.read()
            encoded = base64.encodebytes(content)
            details[filename] = encoded.decode("ascii")
    return details


def insert_single_item(handles, table_name, item, id_key):
    assert isinstance(item, dict)
    item.pop("_id", None)
    if id_key is not None:
        assert id_key in item
        r = query_single_table_cached(handles, table_name, item[id_key], id_key)
        if len(r) > 0:
            raise ValueError("duplicate id")

    derived_rule = config.derived_sharding_rules.get(table_name)
    if derived_rule is not None:
        local_key, shard_table_name, foreign_key = derived_rule
        assert local_key in item
        r = query_single_table_cached(
            handles, shard_table_name, {foreign_key: item[local_key]}, foreign_key
        )
        assert len(r) == 1
        shard_item = r[0]
    else:
        # regular sharding
        shard_table_name = table_name
        shard_item = item

    sharding_rule = config.sharding_rules.get(shard_table_name)
    if sharding_rule is not None:
        shard_key, location_map = sharding_rule
        assert shard_key in shard_item
        assert shard_item[shard_key] in location_map
        locations = location_map[shard_item[shard_key]]
    else:
        # default behavior: each db site hold a replica
        locations = [name for name in config.component_names if name.startswith("dbms")]

    for dbms, cache in config.dbms_nodes:
        # invalidate cache
        if id_key is not None:
            handles[cache].hdel(table_name, item[id_key])
        handles[cache].delete(table_name + "_query")
        if dbms in locations:
            # print("insert OK", dbms, table_name, item)
            handles[dbms][table_name].insert_one(item)


def update_single_item(handles, table_name, new_item, id_key):
    assert isinstance(new_item, dict)
    assert id_key is not None and id_key in new_item
    new_item.pop("_id", None)

    item_id = new_item[id_key]
    r = query_single_table_cached(handles, table_name, item_id, id_key)
    if len(r) != 1:
        raise ValueError("trying to update multiple documents / no match")
    old_item = r[0]

    if table_name in config.derived_sharding_rules:
        raise NotImplementedError()  # disallow updates for now

    sharding_rule = config.sharding_rules.get(table_name)
    if sharding_rule is not None:
        shard_key, location_map = sharding_rule
        assert shard_key in new_item
        if new_item[shard_key] != old_item[shard_key]:
            raise NotImplementedError("cannot modify shard field")

        locations = location_map[old_item[shard_key]]
    else:
        # default behavior: each db site hold a replica
        locations = [name for name in config.component_names if name.startswith("dbms")]

    for dbms, cache in config.dbms_nodes:
        # invalidate cache
        handles[cache].hdel(table_name, item_id)
        handles[cache].delete(table_name + "_query")
        if dbms in locations:
            handles[dbms][table_name].replace_one({id_key: item_id}, new_item)


def remove_items(handles, table_name, condition):
    for dbms, cache in config.dbms_nodes:
        # remove all cache
        handles[cache].delete(table_name)
        handles[cache].delete(table_name + "_query")
        handles[dbms][table_name].delete_many(condition)


def convert_object_id(obj):
    # by default ObjectId is not JSON serializable
    # this function recursively converts ObjectId values in `obj` to their string forms
    keys = None
    if isinstance(obj, list):
        keys = range(len(obj))
    elif isinstance(obj, dict):
        keys = obj.keys()

    if keys is not None:
        for k in keys:
            v = obj[k]
            if isinstance(v, ObjectId):
                obj[k] = str(v)
            elif isinstance(v, (list, dict)):
                obj[k] = convert_object_id(v)
    return obj

'''
Below are the RESTful API endpoints
'''

DDBS = Flask(__name__)
handles = init()


@DDBS.route("/")
def ddbs_get_home():
    return "DDBS"


@DDBS.route("/users", methods=["GET"])
def ddbs_get_all_users():
    '''
    GET: get all users, bypassing cache
    '''
    users = query_single_table_direct(handles, "user", None, "uid")
    return users


@DDBS.route("/user/<int:uid>", methods=["GET", "POST"])
def ddbs_get_user(uid):
    '''
    GET: get user info, using cache
    POST: update user info with form data (timestamp, uid, obtainedCredits) converted to int, using cache
    '''
    users = query_single_table_cached(handles, "user", uid, "uid")
    if request.method == "GET":
        return users
    elif request.method == "POST":
        user_transforms = to_int_transforms(["timestamp", "uid", "obtainedCredits"])
        user_item = map_document(dict(request.form), user_transforms)
        if len(users) > 0:
            update_single_item(handles, "user", user_item, "uid")
        else:
            insert_single_item(handles, "user", user_item, "uid")
        return ""
    return ""


@DDBS.route("/articles", methods=["GET"])
def ddbs_get_all_articles():
    '''
    GET: get all articles, bypassing cache
    '''
    articles = query_single_table_direct(handles, "article", None, "aid")
    return articles


@DDBS.route("/article/<int:aid>", methods=["GET"])
def ddbs_get_article(aid):
    '''
    GET: get article info, using cache
    '''
    articles = query_single_table_cached(handles, "article", aid, "aid")
    return articles


@DDBS.route("/user_read/<int:uid>", methods=["GET"])
def ddbs_get_user_read(uid):
    '''
    GET: get user read history, using cache

    Complex query: join user, read, article
    '''
    user_reads = query_user_read(handles, {"uid": uid})
    return convert_object_id(user_reads)


@DDBS.route("/popular/<int:timestamp_ms>", methods=["GET"])
def ddbs_get_popular_rank(timestamp_ms):
    '''
    GET: get popular articles, using cache
    
    First query information from be-read collection, then get article details(text, image, video) from HDFS
    '''
    popular_info = query_popular_articles(handles, timestamp_ms)
    # NOTE: resource files are base64 encoded and they are large
    for temporal_gran, article_info in popular_info.items():
        for item in article_info:
            item["details"] = fetch_article_details(handles["hdfs"], item["article"])
    return popular_info


@DDBS.route("/status", methods=["GET"])
def ddbs_get_status():
    '''
    GET: get size of each collection and cache, excluding temporary collections
    '''
    status_list = []
    for site, cache in config.dbms_nodes:
        db, kv = handles[site], handles[cache]

        collection_sizes = {}
        cache_sizes = {}
        for coll_name in db.list_collection_names():
            collection_sizes[coll_name] = db[coll_name].count_documents({})
            if not coll_name.startswith("tmp"):
                item_cache_key, query_cache_key = coll_name, coll_name + "_query"
                cache_sizes[item_cache_key] = kv.hlen(item_cache_key)  # item cache
                cache_sizes[query_cache_key] = kv.hlen(query_cache_key)  # query cache

        status = dict(site=site, collections=collection_sizes, caches=cache_sizes)
        status_list.append(status)

    # print(json.dumps(status_list, indent=4, separators=(",", ": ")))
    return status_list


@DDBS.route("/flush_cache", methods=["POST"])
def ddbs_flush_cache():
    for dbms, cache in config.dbms_nodes:
        coll_names = handles[dbms].list_collection_names()
        for name in coll_names:
            handles[cache].delete(name)
            handles[cache].delete(name + "_query")
    return ""


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--init",
        action="store_true",
        required=False,
        default=False,
        help="if set, bulk insert data into databases & hdfs",
    )
    args = parser.parse_args()

    print("Successfully init handles.")

    if args.init:
        init_database_tables(handles)
        init_hdfs_content(handles["hdfs"])
        print("Successfully init databases.")


    DDBS.run(debug="true", host="127.0.0.1", port="23333")

    # from IPython import embed
    # embed()
