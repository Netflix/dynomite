#!/usr/bin/env python3
import redis
import random

class DynoCluster(object):
    def __init__(self, nodes):
        self.nodes = tuple(nodes)

    def get_connection(self):
        node = random.choice(self.nodes)
        return node.get_connection()
