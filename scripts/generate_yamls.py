#!/usr/bin/python

'''
script for generating dynomite yaml files for every node in a cluster.
tokens randomly generated, with a variable count per node, as well.

usage: <script> publicIp:dc_name publicIp:dc_name publicIp:dc_name ...
outputs one yaml file per input node
'''

import yaml, sys, random

APPNAME='dyn_o_mite'
CLIENT_PORT='8102'
DYN_PEER_PORT=8101
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
    # get the peers ready, and yank the current one from the dict
    dyn_seeds_map = token_map.copy()
    del dyn_seeds_map[k]
    dyn_seeds = []
    for y,z in dyn_seeds_map.items():
        key = y.split(':')
        dyn_seeds.append(key[0] + ':' + str(DYN_PEER_PORT) + ':' + key[1] + ':' + z);

    ip_dc = k.split(':');
    data = {
        'listen': '0.0.0.0:' + CLIENT_PORT,
        'hash': 'murmur',
        'distribution': 'vnode',
        'preconnect': True,
        'auto_eject_hosts': False,
        'server_retry_timeout': 200000,
        'timeout': 150000,
        'servers': ['127.0.0.1:' + MEMCACHE_PORT + ':1'],
        'dyn_seed_provider': 'simple_provider',

        'dyn_port': DYN_PEER_PORT,
        'dyn_listen': '0.0.0.0:' + str(DYN_PEER_PORT),
        'dyn_read_timeout': 200000,
        'dyn_write_timeout': 200000,
        'datacenter': ip_dc[1],
        'tokens': v,
        'dyn_seeds': dyn_seeds,
        }

    outer = {APPNAME: data}
    file_name = ip_dc[0] + '.yml'
    with open(file_name, 'w') as outfile:
        outfile.write( yaml.dump(outer, default_flow_style=False) )

    scp_host_tag = ''
    if "east" in ip_dc[1]:
        scp_host_tag = '.compute-1.amazonaws.com'
    else:
        scp_host_tag = '.' + ip_dc[1] + '.compute.amazonaws.com'
    
    print 'oq-scp -r ' + ip_dc[1] + ' ' + file_name + ' ec2-' + ip_dc[0].replace('.', '-') + scp_host_tag + ':dynomite/conf/dynomite.yml'
