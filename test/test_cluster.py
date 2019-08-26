#!/usr/bin/env python3
from contextlib import ExitStack
from time import sleep
from urllib.request import urlopen
import argparse
import json
import random
import sys
import yaml

from plumbum import local

from dyno_cluster import DynoCluster, DynoSpec
from func_test import comparison_test
from utils import generate_ips, setup_temp_dir, sleep_with_animation
from redis_node import RedisNode

REDIS_PORT = 1212
STATS_PORT = 22222
SETTLE_TIME = 3

def main():
    parser = argparse.ArgumentParser(
        description='Autogenerates a Dynomite cluster and runs functional ' +
            'tests against it')
    parser.add_argument('dynospec_file', default='test/dyno_spec_file.yaml',
        help='YAML file describing desired cluster', nargs='?')
    args = parser.parse_args()

    # Setup a temporary directory to store logs and configs for this cluster.
    temp = setup_temp_dir()

    specs = parse_dynospec_file(args.dynospec_file)

    # Create a standalone Redis node.
    standalone_redis_ip = redis_ip(len(specs))
    standalone_redis = RedisNode(standalone_redis_ip, REDIS_PORT)

    # Create a Dynomite cluster.
    dynomite_cluster = DynoCluster.fromDynomiteSpecs(specs)

    with ExitStack() as stack:
        # Make sure to change the working directory to the temp dir before running the
        # tests.
        stack.enter_context(local.cwd(temp))
        # Launch the standalone Redis node and the dynomite cluster.
        stack.enter_context(standalone_redis)
        stack.enter_context(dynomite_cluster)

        # Wait for a while for the above nodes to start.
        sleep_with_animation(SETTLE_TIME, "Waiting for cluster to start")

        # Run all the functional comparison tests.
        comparison_test(standalone_redis, dynomite_cluster, False)

        random_node = random.choice(dynomite_cluster.nodes)
        stats_url = 'http://{}:{}/info'.format(random_node.ip, STATS_PORT)
        json.loads(urlopen(stats_url).read().decode('ascii'))


def redis_ip(dyno_node_count):
    assert dyno_node_count < 254
    return "127.0.0.254"


def parse_dynospec_file(filename):
    with open(filename, 'r') as f:
        specs = yaml.safe_load(f)
    return [DynoSpec(**dct) for dct in specs]


if __name__ == '__main__':
    sys.exit(main())
