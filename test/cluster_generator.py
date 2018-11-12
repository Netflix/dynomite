#!/usr/bin/env python3
from contextlib import ExitStack
from tempfile import mkdtemp
from time import sleep
from urllib.request import urlopen
import argparse
import json
import random
import sys

from plumbum import LocalPath
from plumbum import local

from dyno_cluster import DynoCluster
from func_test import comparison_test
from utils import generate_ips
from redis_node import RedisNode

REDIS_PORT = 1212
STATS_PORT = 22222
SETTLE_TIME = 5

def setup_temp_dir():
    tempdir = LocalPath(mkdtemp(dir='.', prefix='test_run.'))
    (tempdir / 'logs').mkdir()
    confdir = (tempdir / 'conf')
    confdir.mkdir()

    LocalPath('../../conf/dynomite.pem').symlink(confdir / 'dynomite.pem')

    return tempdir

def main():
    parser = argparse.ArgumentParser(
        description='Autogenerates a Dynomite cluster and runs functional ' +
            'tests against it')
    parser.add_argument('request_file', default='test/request.yaml',
        help='YAML file describing desired cluster', nargs='?')
    args = parser.parse_args()

    # Setup a temporary directory to store logs and configs for this cluster.
    temp = setup_temp_dir()

    # Generate IP addresses to be used by the nodes we will create.
    ips = generate_ips()

    # Create a standalone Redis node.
    standalone_redis_ip = next(ips)
    standalone_redis = RedisNode(standalone_redis_ip, REDIS_PORT)

    # Create a Dynomite cluster.
    dynomite_cluster = DynoCluster(args.request_file, ips)

    with ExitStack() as stack:
        # Make sure to change the working directory to the temp dir before running the
        # tests.
        stack.enter_context(local.cwd(temp))
        # Launch the standalone Redis node and the dynomite cluster.
        stack.enter_context(standalone_redis)
        stack.enter_context(dynomite_cluster)

        # Wait for a while for the above nodes to start.
        sleep(SETTLE_TIME)

        # Run all the functional comparison tests.
        comparison_test(standalone_redis, dynomite_cluster, False)

        random_node = random.choice(dynomite_cluster.nodes)
        stats_url = 'http://{}:{}/info'.format(random_node.ip, STATS_PORT)
        json.loads(urlopen(stats_url).read().decode('ascii'))

if __name__ == '__main__':
    sys.exit(main())
