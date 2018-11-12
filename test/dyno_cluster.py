#!/usr/bin/env python3
from collections import namedtuple
import redis
import random
import yaml

from dyno_node import DynoNode
from utils import *

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

class DynoSpec(namedtuple('DynoSpec', 'ip port dc rack token '
    'local_connections remote_connections seed_string')):
    """Specifies how to launch a dynomite node"""

    def __new__(cls, ip, port, rack, dc, token, local_connections,
                remote_connections):
        seed_string = '{}:{}:{}:{}:{}'.format(ip, port, rack, dc, token)
        return super(DynoSpec, cls).__new__(cls, ip, port, rack, dc, token,
            local_connections, remote_connections, seed_string)

    def __init__(self, ip, port, rack, dc, token, local_connections,
                 remote_connections):
        self.dnode_port = INTERNODE_LISTEN
        self.data_store_port = REDIS_PORT
        self.stats_port = STATS_PORT

    def _generate_config(self, seeds_list):
        conf = dict(DYN_O_MITE_DEFAULTS)
        conf['datacenter'] = self.dc
        conf['rack'] = self.rack
        dyn_listen = '{}:{}'.format(self.ip, self.dnode_port)
        conf['dyn_listen'] = dyn_listen
        conf['listen'] = '{}:{}'.format(self.ip, self.port)

        # filter out our own seed string
        conf['dyn_seeds'] = [s for s in seeds_list if s != self.seed_string]
        conf['servers'] = ['{}:{}:0'.format(self.ip, self.data_store_port)]
        conf['stats_listen'] = '{}:{}'.format(self.ip, self.stats_port)
        conf['tokens'] = self.token
        conf['local_peer_connections'] = self.local_connections
        conf['remote_peer_connections'] = self.remote_connections
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
        tokens = tokens_for_cluster(self.request, None)
        counts_by_rack = dict_request(self.request)
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
                    yield DynoSpec(ip, CLIENT_LISTEN, dc, rack, token,
                        local_count, remote_count)

    def launch(self):
        # Get the list of seeds from the specification for each node.
        seeds_list = [spec.seed_string for spec in self.specs]
        # Launch each individual Dyno node.
        self.nodes = [DynoNode(spec, seeds_list) for spec in self.specs]
        for n in self.nodes:
            n.launch()

    def teardown(self):
        for n in self.nodes:
            n.teardown()

    def __enter__(self):
        self.launch()

    def __exit__(self, type_, value, traceback):
        self.teardown()

    def get_connection(self):
        node = random.choice(self.nodes)
        return node.get_connection()
