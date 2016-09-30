#!/usr/bin/python

'''
script for generating dynomite yaml files for every node in a cluster.
tokens randomly generated, with a variable count per node, as well.

usage: <script> publicIp:rack_name publicIp:rack_name publicIp:rack_name ...
outputs one yaml file per input node
'''

import yaml, sys, random

APPNAME='dyn_o_mite'
CLIENT_PORT='8102'
DYN_PEER_PORT=8101
MEMCACHE_PORT='11211'
MAX_TOKEN = 4294967295

DEFAULT_DC = 'default_dc'

# gen map of node to random count (3-7) of random tokens (0-MAX_INT)
token_map = dict()
token_item = (MAX_TOKEN // (len(sys.argv) -1))
for i in range(1, len(sys.argv)):
    node = sys.argv[i]
    tokens = []
    token_value = (token_item * i)
    if token_value > MAX_TOKEN:
        token_value = MAX_TOKEN

    tokens.append(token_value)

    tok_str = ','.join(str(it) for it in tokens)
    token_map[node] = tok_str

for k,v in token_map.items():
    # get the peers ready, and yank the current one from the dict
    dyn_seeds_map = token_map.copy()
    del dyn_seeds_map[k]
    dyn_seeds = []
    for y,z in dyn_seeds_map.items():
        key = y.split(':')
        dyn_seeds.append(key[0] + ':' + str(DYN_PEER_PORT) + ':' + key[1] + ':' + DEFAULT_DC + ':' + z);

    ip_dc = k.split(':');
    data = {
        'listen': '0.0.0.0:' + CLIENT_PORT,
        'timeout': 150000,
        'servers': ['127.0.0.1:' + MEMCACHE_PORT + ':1'],
        'dyn_seed_provider': 'simple_provider',

        'dyn_port': DYN_PEER_PORT,
        'dyn_listen': '0.0.0.0:' + str(DYN_PEER_PORT),
        'datacenter': DEFAULT_DC,
        'rack': ip_dc[1],
        'tokens': v,
        'dyn_seeds': dyn_seeds,
        }

    outer = {APPNAME: data}
    file_name = ip_dc[0] + '.yml'
    with open(file_name, 'w') as outfile:
        outfile.write( yaml.dump(outer, default_flow_style=False) )
