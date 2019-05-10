#!/usr/bin/env python3
from collections import namedtuple
from plumbum import local
import os
import redis
import random
import shutil
import signal
import yaml

from dyno_node import DynoNode
from utils import *

# TODO: Make this path absolute based on param instead of relative.
CLUSTER_DESC_FILEPATH='../running_cluster.yaml'
DYN_O_MITE_DEFAULTS = dict(
    secure_server_option='datacenter',
    pem_key_file='conf/dynomite.pem',
    data_store=0,
    datastore_connections=1,
)
INTERNODE_LISTEN = 8101
CLIENT_LISTEN = 8102
REDIS_PORT = 1212
STATS_PORT = 22222

class DynoSpec(namedtuple('DynoSpec', 'ip dnode_port client_port rack dc token '
    'local_connections remote_connections seed_string req_conf')):
    """Specifies how to launch a dynomite node"""

    def __new__(cls, ip, dnode_port, client_port, rack, dc, token, local_connections,
                remote_connections, req_conf):
        seed_string = '{}:{}:{}:{}:{}'.format(ip, dnode_port, rack, dc, token)
        return super(DynoSpec, cls).__new__(cls, ip, dnode_port, client_port, rack,
            dc, token, local_connections, remote_connections, seed_string, req_conf)

    def __init__(self, ip, dnode_port, client_port, rack, dc, token, local_connections,
                 remote_connections, req_conf):
        self.data_store_port = REDIS_PORT
        self.stats_port = STATS_PORT

    def _generate_config(self, seeds_list):
        conf = dict(DYN_O_MITE_DEFAULTS)
        conf['datacenter'] = self.dc
        conf['rack'] = self.rack
        dyn_listen = '{}:{}'.format(self.ip, self.dnode_port)
        conf['dyn_listen'] = dyn_listen
        conf['listen'] = '{}:{}'.format(self.ip, self.client_port)

        # filter out our own seed string
        conf['dyn_seeds'] = [s for s in seeds_list if s != self.seed_string]
        conf['servers'] = ['{}:{}:0'.format(self.ip, self.data_store_port)]
        conf['stats_listen'] = '{}:{}'.format(self.ip, self.stats_port)
        conf['tokens'] = self.token
        conf['local_peer_connections'] = self.local_connections
        conf['remote_peer_connections'] = self.remote_connections

        # Add configurations based on the request.
        for conf_key, conf_value in self.req_conf.items():
            conf[conf_key] = conf_value
        return dict(dyn_o_mite=conf)

    def write_config(self, seeds_list):
        config = self._generate_config(seeds_list)
        filename = 'conf/{}:{}:{}.yml'.format(self.dc, self.rack, self.token)
        with open(filename, 'w') as fh:
            yaml.dump(config, fh, default_flow_style=False)
        return filename

class DynoCluster(object):
    def __init__(self, request_file, ips):
        # Load the YAML file describing the cluster.
        with open(request_file, 'r') as fh:
            self.request = yaml.load(fh)

        self.ips = ips
        # Generate the specification for each node to be started in the cluster.
        self.specs = list(self._generate_dynomite_specs())
        self.nodes = []

    def _generate_dynomite_specs(self):
        tokens = tokens_for_cluster(self.request['cluster_desc'], None)
        counts_by_rack = dict_request(self.request['cluster_desc'], 'name', 'racks')
        counts_by_dc = sum_racks(counts_by_rack)
        total_nodes = sum(counts_by_dc.values())
        for dc, racks in tokens:
            dc_count = counts_by_dc[dc]
            rack_count = counts_by_rack[dc]
            remote_count = total_nodes - dc_count
            for rack, tokens in racks:
                local_count = rack_count[rack] - 1
                for token in tokens:
                    ip = next(self.ips)
                    yield DynoSpec(ip, INTERNODE_LISTEN, CLIENT_LISTEN, rack, dc, token,
                        local_count, remote_count, self.request['conf'])

    def _get_cluster_desc_yaml(self):
        yaml_desc = dict(test_dir=str(local.cwd))
        cluster_desc = [dict(name='dyno_nodes')]
        cluster_desc.append(dict(name='redis_nodes'))
        cluster_desc[0]['pids']=[]
        cluster_desc[1]['pids']=[]
        for node in self.nodes:
            cluster_desc[0]['pids'].append(node.get_dyno_node_pid())
            cluster_desc[1]['pids'].append(node.get_storage_node_pid())
        yaml_desc['cluster_desc'] = cluster_desc
        return yaml_desc

    def _pre_launch_sanity_check(self):
        """Checks if there is a cluster already running and tears it down"""
        teardown_running_cluster(CLUSTER_DESC_FILEPATH)

    def _write_running_cluster_file(self):
        yaml_cluster_desc = self._get_cluster_desc_yaml()
        with open(CLUSTER_DESC_FILEPATH, 'w') as outfile:
            yaml.dump(yaml_cluster_desc, outfile, default_flow_style=False)

    def _print_cluster_topology(self):
        tokens = tokens_for_cluster(self.request['cluster_desc'], None)
        print("Cluster topology:-");
        for dc, racks in tokens:
            print("\tDC: %s" % dc)
            for rack, tokens in racks:
                print("\t\tRack: %s" % rack)
                # Nested loop is okay here since the #nodes will always be small.
                for node in self.nodes:
                    if node.spec.dc == dc and node.spec.rack == rack:
                        print("\t\t\tNode: %s  || PID: %s" % (node.name, \
                            node.get_dyno_node_pid()))

    def _delete_running_cluster_file(self):
        os.remove(CLUSTER_DESC_FILEPATH)

    def launch(self):
        self._pre_launch_sanity_check()
        # Get the list of seeds from the specification for each node.
        seeds_list = [spec.seed_string for spec in self.specs]
        # Launch each individual Dyno node.
        self.nodes = [DynoNode(spec, seeds_list) for spec in self.specs]
        for n in self.nodes:
            n.launch()

        # Now that the cluster is up, write its description to a file.
        self._write_running_cluster_file()
        self._print_cluster_topology()

    def teardown(self):
        for n in self.nodes:
            n.teardown()

        # Delete the cluster description file if it exists.
        self._delete_running_cluster_file()

    def __enter__(self):
        self.launch()

    def __exit__(self, type_, value, traceback):
        self.teardown()

    def get_connection(self):
        node = random.choice(self.nodes)
        return node.get_connection()
