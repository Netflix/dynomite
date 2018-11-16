#!/usr/bin/env python3
import sys

from utils import teardown_running_cluster

def main():
    teardown_running_cluster('running_cluster.yaml')

if __name__ == '__main__':
    sys.exit(main())
