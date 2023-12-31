localhost = "127.0.0.1"

component_names = ("dbms0", "dbms1", "cache0", "cache1", "hdfs")

component_addresses = {
    "cache0": (localhost, 20000),
    "dbms0": (localhost, 20001),
    "cache1": (localhost, 20002),
    "dbms1": (localhost, 20003),
    "hdfs": (localhost, 9870),
}

dbms_nodes = (
    ("dbms0", "cache0"),
    ("dbms1", "cache1"),
)

temporal_granularities = ((1, "daily"), (7, "weekly"), (30, "monthly"))

sharding_rules = {
    # table_name: (shard_key, {value1: sites1, value2: sites2, ...})
    "user": ("region", {"Beijing": ["dbms0"], "Hong Kong": ["dbms1"]}),
    "article": ("category", {"science": ["dbms0", "dbms1"], "technology": ["dbms1"]}),
    "popular_rank": (
        "temporalGranularity",
        {"daily": ["dbms0"], "weekly": ["dbms1"], "monthly": ["dbms1"]},
    ),
}

derived_sharding_rules = {
    # table_name: (local_key, foreign_table, foreign_key)
    "read": ("uid", "user", "uid"),
    "be_read": ("aid", "article", "aid"),
}

id_key_mapping = {
    "user": "uid",
    "article": "aid",
    "read": "id",
    "be_read": "id",
    "popular_rank": "id",
}
