#!/usr/bin/python

'''
script for generating dynomite yaml files for every node in a cluster.
This script should be run per rack for all nodes in the rack and so the tokens are equally distributed.
usage: <script> publicIp:rack_name publicIp:rack_name publicIp:rack_name ...
outputs one yaml file per input node(for a single rack)
restric generation of the confs for all hosts per rack and not across rack.
'''

import yaml, sys

APPNAME='dyn_o_mite'
CLIENT_PORT='8102'
DYN_PEER_PORT=8101
MEMCACHE_PORT='11211'
MAX_TOKEN = 4294967295

DEFAULT_DC = 'default_dc'

# generate the equidistant tokens for the number of nodes given. max 4294967295
token_map = dict()
token_item = (MAX_TOKEN // (len(sys.argv) -1))
for i in range(1, len(sys.argv)):
    node = sys.argv[i]
    token_value = (token_item * i)
    if token_value > MAX_TOKEN:
        token_value = MAX_TOKEN

    token_map[node] = token_value

for k,v in token_map.items():
    # get the peers ready, and yank the current one from the dict
    dyn_seeds_map = token_map.copy()
    del dyn_seeds_map[k]
    dyn_seeds = []
    for y,z in dyn_seeds_map.items():
        key = y.split(':')
        dyn_seeds.append(key[0] + ':' + str(DYN_PEER_PORT) + ':' + key[1] + ':' + DEFAULT_DC + ':' + str(z));

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
