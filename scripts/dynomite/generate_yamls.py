#!/usr/bin/env python3

'''
script for generating dynomite yaml files for every node in a cluster.
This script should be run per rack for all nodes in the rack and so the tokens are equally distributed.
usage: <script> publicIp:rack_name publicIp:rack_name publicIp:rack_name ...
outputs one yaml file per input node(for a single rack)
restric generation of the confs for all hosts per rack and not across rack.
'''

import yaml, sys
import argparse


APPNAME='dyn_o_mite'
DEFAULT_CLIENT_PORT='8102'
DYN_PEER_PORT=8101
MEMCACHE_PORT='11211'
REDIS_PORT='6379'
MAX_TOKEN = 4294967295

DEFAULT_DC = 'default_dc'




if __name__ == '__main__':

    parser = argparse.ArgumentParser("""
        Dynomite Configuration YAML Generator V0.2
        
        Script for generating dynomite yaml files for every node in a cluster.
        This script should be run per rack for all nodes in the rack and so the tokens are equally distributed.
        
    """)

    parser.add_argument('nodes', type=str, nargs='+',
                        help="""
            publicIp:rack_name publicIp:rack_name publicIp:rack_name ...

            outputs one yaml file per input node(for a single rack)
            restric generation of the confs for all hosts per rack and not across rack.
        """)


    parser.add_argument(
        '-dc',
        dest='datacenter_name',
        default=DEFAULT_DC,
        help="""
            Name of the datacenter this rack belongs to
        """
    )

    parser.add_argument(
        '-cp',
        dest='client_port',
        type=str,
        default=DEFAULT_CLIENT_PORT,
        help="""
            Client port to use (The client port Dynomite provides instead of directly accessing redis or memcache) 
            Default is: {}
        """.format(DEFAULT_CLIENT_PORT)
    )

    parser.add_argument(
        '-sp',
        dest='server_port',
        type=str,
        default=REDIS_PORT,
        help="""
            The port your redis or memcache will run locally on each node, assuming it's uniform, (default is: {})
        """.format(REDIS_PORT)
    )


    parser.add_argument(
        '--redis',
        action="store_true",
        default=True,
        help="""
            Sets the data_store property to use redis (0).
        """
    )

    parser.add_argument(
        '--mem',
        action="store_true",
        default=False,
        help="""
            Sets the data_store property to use memcache (1).
        """
    )


    args = parser.parse_args()


    if (args.mem):
        data_store = 1
    else:
        data_store = 0

    # generate the equidistant tokens for the number of nodes given. max 4294967295
    token_map = dict()
    total_nodes = len(args.nodes)
    token_item = (MAX_TOKEN // total_nodes)

    for i in range(0, total_nodes):
        node = args.nodes[i]
        #print "generating node file: {}.yml".format(node)
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
            dyn_seeds.append(key[0] + ':' + str(DYN_PEER_PORT) + ':' + key[1] + ':' + args.datacenter_name + ':' + str(z));

        ip_dc = k.split(':')
        data = {
            'data_store': data_store,
            'listen': '0.0.0.0:' + args.client_port,
            'timeout': 150000,
            'servers': ['127.0.0.1:' + args.server_port + ':1'],
            'dyn_seed_provider': 'simple_provider',

            'dyn_port': DYN_PEER_PORT,
            'dyn_listen': '0.0.0.0:' + str(DYN_PEER_PORT),
            'datacenter': args.datacenter_name,
            'rack': ip_dc[1],
            'tokens': v,
            'dyn_seeds': dyn_seeds,
            }

        outer = {APPNAME: data}

        file_name = ip_dc[0] + '.yml'
        with open(file_name, 'w') as outfile:
            outfile.write( yaml.dump(outer, default_flow_style=False) )
