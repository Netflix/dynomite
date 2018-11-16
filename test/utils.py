#!/usr/bin/env python3
import random
import os
import signal
import string
import yaml

from ip_util import int2quad
from ip_util import quad2int
from pathlib import Path
from plumbum import LocalPath
from tempfile import mkdtemp

from itertools import count

BASE_IPADDRESS = quad2int('127.0.1.1')
RING_SIZE = 2**32


def string_generator(size=6, chars=string.ascii_letters + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def number_generator(size=4, chars=string.digits):
    return ''.join(random.choice(chars) for _ in range(size))

def dict_request(request, key1, key2):
    """Converts the request into an easy to consume dict format."""

    # We don't ingest the request in this format originally because dict
    # ordering is nondeterministic, and one of our goals is to deterministically
    # generate clusters.
    # TODO: Python 3.6+ has ordered dicts, so the above doesn't hold true from that
    # version.
    return dict(
        (elem[key1], dict(elem[key2]))
        for elem in request
    )

def teardown_running_cluster(cluster_desc_filepath, delete_test_dir=False):
    """Checks if there is a cluster already running based on 'cluster_desc_filepath'
        and tears it down"""
    running_cluster_file = Path(cluster_desc_filepath)
    if running_cluster_file.is_file():
        with open(cluster_desc_filepath, 'r') as fh:
            yaml_desc = yaml.load(fh)
            for cluster in yaml_desc['cluster_desc']:
                print("Killing existing '{}'".format(cluster['name']))
                for pid in cluster['pids']:
                    try:
                        os.kill(pid, signal.SIGTERM)
                    except ProcessLookupError as e:
                        print("PID: {} | Cause: {}".format(pid, e))
            if delete_test_dir:
                print("Deleting test directory of cluster:{}".format(
                    yaml_desc['test_dir']))
                # Delete the test directory of this cluster.
                shutil.rmtree(yaml_desc['test_dir'])
        os.remove(cluster_desc_filepath)

def setup_temp_dir():
    tempdir = LocalPath(mkdtemp(dir='.', prefix='test_run.'))
    (tempdir / 'logs').mkdir()
    confdir = (tempdir / 'conf')
    confdir.mkdir()

    LocalPath('../../conf/dynomite.pem').symlink(confdir / 'dynomite.pem')

    return tempdir

def sum_racks(dcs):
    return dict(
        (name, sum(racks.values()))
        for name, racks in dcs.items()
    )

def pick_tokens(count, start_offset):
    stride = RING_SIZE // count
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
    for ip in count(start=BASE_IPADDRESS, step=1):
        yield int2quad(ip)
