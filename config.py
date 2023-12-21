
localhost = '127.0.0.1'

component_names = [
    'dbms0', 'dbms1', 'cache0', 'cache1'
]

component_addresses = {
    'cache0': (localhost, 20000),
    'dbms0': (localhost, 20001),
    'cache1': (localhost, 20002),
    'dbms1': (localhost, 20003),
}

sharding_rules = {
    'user': ('region', {'Beijing': ['dbms0'], 'Hong Kong': ['dbms1']}),
    'article': ('category', {'science': ['dbms0', 'dbms1'], 'technology': ['dbms1']}),
    'popular_rank': ('temporalGranularity', {'daily': ['dbms0'], 'weekly': ['dbms1'], 'monthly': ['dbms1']}),
}
