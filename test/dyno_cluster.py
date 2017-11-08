#!/usr/bin/python
import redis
import random

class DynoCluster(object):
    def __init__(self, nodes):
        self.nodes = nodes
    def get_connection(self):
        node = random.choice(self.nodes)
        return node.get_connection()
