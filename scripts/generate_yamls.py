#!/usr/bin/python

'''
script for generating dynomite yaml files for every node in a cluster.
tokens randomly generated, with a variable count per node, as well.

usage: <script> publicIp:dc_name publicIp:dc_name publicIp:dc_name ...
outputs one yaml file per input node
'''

import yaml, sys, random

APPNAME='dyn_o_mite'
CLIENT_PORT='22200'
DYN_PEER_PORT='32200'
MEMCACHE_PORT='11211'


# gen map of node to random count (3-7) of random tokens (0-MAX_INT)
token_map = dict()
for i in range(1, len(sys.argv)):
    node = sys.argv[i]
    token_cnt = random.randrange(2,7, 1)
    tokens = []
    for j in range(token_cnt):
        t = random.randint(0,4294967295)
        tokens.append(t)
    tokens.sort()

    tok_str = ','.join(str(it) for it in tokens)
    token_map[node] = tok_str

for k,v in token_map.items():
    ip_dc = k.split(':');

    # get the peers ready, and yank the current one from the dict
    dyn_seeds_map = token_map.copy()
    del dyn_seeds_map[k]
    dyn_seeds = []
    for y,z in dyn_seeds_map.items():
        dyn_seeds.append(y + ':' + z);

    data = {
        'listen': ip_dc[0] + ':' + CLIENT_PORT,
        'hash': 'murmur',
        'distribution': 'vnode',
        'preconnect': True,
        'auto_eject_hosts': False,
        'server_retry_timeout': 200000,
        'timeout': 150000,
        'servers': [ip_dc[0] + ':' + MEMCACHE_PORT],
        'dyn_seed_provider': 'simple_provider',

        'dyn_port': 32200,
        'dyn_listen': ip_dc[0] + ':' + DYN_PEER_PORT,
        'dyn_read_timeout': 200000,
        'dyn_write_timeout': 200000,
        'datacenter': ip_dc[1],
        'tokens': v,
        'dyn_seeds': dyn_seeds,
        }

    outer = {APPNAME: data}
    with open(ip_dc[0] + '.yml', 'w') as outfile:
        outfile.write( yaml.dump(outer, default_flow_style=False) )

