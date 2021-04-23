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
import os

APPNAME='dyn_o_mite'
DEFAULT_CLIENT_PORT='8102'
DYN_PEER_PORT= '8101'
MEMCACHE_PORT='11211'
REDIS_PORT='6379'
MAX_TOKEN = 4294967295

SECURE_SERVER_OPTION = "datacenter"
READ_CONSISTENCY = "DC_ONE"



if __name__ == '__main__':

    parser = argparse.ArgumentParser("""
        Dynomite Configuration YAML Generator
        
        Script for generating Dynomite yaml configuration files for distribution with every node.
        generated yaml files will be outputted for each node, named as {ipaddress}.yml
        so cluster wide can be easily configured
        
    """)

    parser.add_argument('nodes', type=str, nargs='+',
                        help="""
            Usage: <script> publicIp:rack_name:datacenter publicIp:rack_name:datacenter ...

            outputs one yaml file per input node(for a single rack)
            restrict generation of the confs for all hosts per rack and not across rack.
        """)


    parser.add_argument(
        '-cp',
        dest='client_port',
        type=str,
        default=DEFAULT_CLIENT_PORT,
        help="""
            Client port to use (The client port Dynomite provides instead of directly accessing redis or memcache) 
            Default is: {}
            
            Your redis or memcache clients should connect to this port
        """.format(DEFAULT_CLIENT_PORT)
    )

    parser.add_argument(
        '-o',
        dest='output_dir',
        type=str,
        default='./',
        help="""
            Output directory for the YAML files, if does not exist will be created, Default is current directory (.)
        """
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
        '-pp',
        dest='peer_port',
        type=str,
        default=DYN_PEER_PORT,
        help="""
            The port Dynamo clients will use to communicate with each other, (default is: {})
        """.format(DYN_PEER_PORT)
    )

    parser.add_argument(
        '-rc',
        dest='read_consistency',
        type=str,
        default=READ_CONSISTENCY,
        choices=set(('DC_ONE', 'DC_QUORUM', 'DC_SAFE_QUORUM')),
        help="""
            Sets the read_consistency of the cluster operation mode (default is: DC_ONLY)
        """
    )

    parser.add_argument(
        '-sso',
        dest='secure_server_option',
        type=str,
        default=SECURE_SERVER_OPTION,
        choices=set(('none', 'rack', 'datacenter')),
        help="""
            Type of communication between Dynomite nodes, Must be one of 'none', 'rack', 'datacenter', or 'all' (default is: {})
        """.format(SECURE_SERVER_OPTION)
    )


    parser.add_argument(
        '--redis',
        action="store_true",
        default=True,
        help="""
            Sets the data_store property to use redis (0), Default is 0.
        """
    )

    parser.add_argument(
        '--mem',
        action="store_true",
        default=False,
        help="""
            Sets the data_store property to use memcache (1), Default is 0.
        """
    )


    args = parser.parse_args()


    dir = os.path.dirname(__file__)

    output_path = args.output_dir

    if os.path.isabs(dir):
        print "path exists:{}".format(dir)
    else:
        output_path = os.path.join(dir, args.output_dir)
        if (not os.path.exists(output_path)):
            os.makedirs(output_path)
            print "creating output path %s" % output_path




    if (args.mem):
        data_store = 1
    else:
        data_store = 0

    # generate the equidistant tokens for the number of nodes given. max 4294967295
    token_map = dict()
    total_nodes = len(args.nodes)
    token_item = (MAX_TOKEN // total_nodes)

    #print "token_item:{}".format(token_item)
    for i in range(0, total_nodes):
        node = args.nodes[i]
        #print "Iterating node file ... > {}.yml".format(node)
        token_value = (token_item * (i+1))
        if token_value > MAX_TOKEN:
            token_value = MAX_TOKEN

        token_map[node] = token_value

    for k,v in token_map.items():
        # get the peers ready, and yank the current one from the dict
        #print "k:{} v:{}".format(k,v)
        dyn_seeds_map = token_map.copy()
        del dyn_seeds_map[k]
        dyn_seeds = []
        for y,z in dyn_seeds_map.items():
            key = y.split(':')
            dyn_seeds.append(key[0] + ':' + str(args.peer_port) + ':' + key[1] + ':' + key[2] + ':' + str(z))

        ip_dc = k.split(':')
        #print ip_dc
        data = {
            'data_store': data_store,
            'listen': '0.0.0.0:' + args.client_port,
            'timeout': 150000,
            'servers': ['127.0.0.1:' + args.server_port + ':1'],
            'dyn_seed_provider': 'simple_provider',
            'read_consistency': 'DC_ONE',
            'secure_server_option': args.secure_server_option,
            'dyn_port': args.peer_port,
            'dyn_listen': '0.0.0.0:' + args.peer_port,
            'datacenter': ip_dc[2],
            'rack': """{}""".format(ip_dc[1]),
            'tokens': """{}""".format(v),
            'dyn_seeds': dyn_seeds,
            }

        outer = {APPNAME: data}

        file_name = """{}.yml""".format(os.path.join(output_path, ip_dc[0]))

        with open(file_name, 'w') as outfile:
            outfile.write( yaml.dump(outer, default_flow_style=False))
