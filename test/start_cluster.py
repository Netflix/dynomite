#!/usr/bin/env python3
import argparse
import sys

from plumbum import local
from time import sleep

from dyno_cluster import DynoCluster
from utils import generate_ips, setup_temp_dir

SETTLE_TIME = 5

def main():
    parser = argparse.ArgumentParser(
        description='Autogenerates a Dynomite cluster and runs functional ' +
            'tests against it')
    parser.add_argument('request_file', default='test/safe_quorum_request.yaml',
        help='YAML file describing desired cluster', nargs='?')
    args = parser.parse_args()

    # Setup a temporary directory to store logs and configs for this cluster.
    temp = setup_temp_dir()

    # Generate IP addresses to be used by the nodes we will create.
    ips = generate_ips()

    # Create a Dynomite cluster.
    dynomite_cluster = DynoCluster(args.request_file, ips)

    # Make sure to change the working directory to the temp dir.
    with local.cwd(temp):
        # Launch the dynomite cluster.
        dynomite_cluster.launch()

        # Wait for a while for the above nodes to start.
        sleep(SETTLE_TIME)

if __name__ == '__main__':
    sys.exit(main())
