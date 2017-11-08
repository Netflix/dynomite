#!/usr/bin/env python
from collections import namedtuple
from signal import SIGINT
from tempfile import mkdtemp
from time import sleep
import yaml
import random
import socket
import sys

from plumbum import FG
from plumbum import BG
from plumbum import LocalPath
from plumbum import local

from ip_util import quad2int
from ip_util import int2quad

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
BASE_IPADDRESS = quad2int('127.0.1.1')
RING_SIZE = 2**32

redis = local['redis-server']
dynomite = local['src/dynomite']

def launch_redis(ip):
    logfile = 'logs/redis_{}.log'.format(ip)
    return (redis['--bind', ip, '--port', REDIS_PORT] > logfile) & BG

def pick_tokens(count, start_offset):
    stride = RING_SIZE / count
    token = start_offset
    for i in range(count):
        yield token % RING_SIZE
        token += stride

def tokens_for_rack(count):
    offset = random.randrange(0, RING_SIZE)
    return list(pick_tokens(count, offset))

def tokens_for_dc(racks):
    return [
        (name, tokens_for_rack(count))
        for name, count in racks
    ]

def tokens_for_cluster(dcs, seed):
    if seed is not None:
        random.seed(seed)

    return [
        (dc['name'], tokens_for_dc(dc['racks']))
        for dc in dcs
    ]


def dc_count(dc):
    return sum(count for rack, count in dc)

def generate_ips():
    state = BASE_IPADDRESS
    while True:
        yield int2quad(state)
        state += 1

class DynoSpec(namedtuple('DynoNode', 'ip port dc rack token '
    'local_connections remote_connections seed_string')):

    def __new__(cls, ip, port, rack, dc, token, local_connections,
                remote_connections):
        seed_string = '{}:{}:{}:{}:{}'.format(ip, port, rack, dc, token)
        return super(DynoSpec, cls).__new__(cls, ip, port, rack, dc, token,
            local_connections, remote_connections, seed_string)

    def generate_config(self, seeds_list):
        conf = dict(DYN_O_MITE_DEFAULTS)
        conf['datacenter'] = self.dc
        conf['rack'] = self.rack
        dyn_listen = '{}:{}'.format(self.ip, self.port)
        conf['dyn_listen'] = dyn_listen
        conf['listen'] = '{}:{}'.format(self.ip, CLIENT_LISTEN)

        # filter out our own seed string
        conf['dyn_seeds'] = filter(lambda s: s != self.seed_string, seeds_list)
        conf['servers'] = ['{}:{}:0'.format(self.ip, REDIS_PORT)]
        conf['stats_listen'] = '{}:{}'.format(self.ip, STATS_PORT)
        conf['tokens'] = self.token
        conf['local_peer_connections'] = self.local_connections
        conf['remote_peer_connections'] = self.remote_connections
        return dict(dyn_o_mite=conf)

    def write_config(self, seeds_list):
        config = self.generate_config(seeds_list)
        filename = 'conf/{}:{}:{}.yml'.format(self.dc, self.rack, self.token)
        with open(filename, 'w') as fh:
            yaml.dump(config, fh, default_flow_style=False)
        return filename

    def launch(self, seeds_list):
        config_filename = self.write_config(seeds_list)

        redis_future = launch_redis(self.ip)

        logfile = 'logs/dynomite_{}.log'.format(self.ip)
        # dynomite will exit with status 1 if we SIGINT it
        dynomite_future = dynomite['-o', logfile, '-c', config_filename,
            '-v6'] & BG(1)
        return dynomite_future, redis_future

def dict_request(request):
    return dict(
        (dc['name'], dict(dc['racks']))
        for dc in request
    )

def sum_racks(dcs):
    return dict(
        (name, sum(racks.values()))
        for name, racks in dcs.items()
    )

def generate_nodes(request, ips):
    tokens = tokens_for_cluster(request, None)
    counts_by_rack = dict_request(request)
    counts_by_dc = sum_racks(counts_by_rack)
    total_nodes = sum(counts_by_dc.values())
    for dc, racks in tokens:
        dc_count = counts_by_dc[dc]
        rack_count = counts_by_rack[dc]
        remote_count = total_nodes - dc_count
        for rack, tokens in racks:
            local_count = rack_count[rack] - 1
            for token in tokens:
                ip = next(ips)
                yield DynoSpec(ip, INTERNODE_LISTEN, dc, rack, token,
                    local_count, remote_count)

def main():
    with open('test/request.yaml', 'r') as fh:
        request = yaml.load(fh)

    temp = LocalPath(mkdtemp(dir='.', prefix='test_run.'))
    (temp / 'logs').mkdir()
    confdir = (temp / 'conf')
    confdir.mkdir()

    LocalPath('../../conf/dynomite.pem').symlink(confdir / 'dynomite.pem')

    ips = generate_ips()
    standalone_redis_ip = next(ips)
    nodes = list(generate_nodes(request, ips))
    seeds_list = map(lambda n: n.seed_string, nodes)

    dynomites = []
    redises = []
    with local.cwd(temp):
        redises.append(launch_redis(standalone_redis_ip))
        for n in nodes:
            d, r = n.launch(seeds_list)
            dynomites.append(d)
            redises.append(r)

    sleep(5)
    try:
        local['test/func_test.py'] & FG
        local['test/supplemental.sh'] & FG
    finally:
        for f in dynomites:
            f.proc.send_signal(SIGINT)

        for f in redises:
            f.proc.send_signal(SIGINT)

        for f in dynomites:
            f.wait()

        for f in redises:
            f.wait()

if __name__ == '__main__':
    sys.exit(main())
