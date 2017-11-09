#!/usr/bin/env python3
import redis
from node import Node
from redis_node import RedisNode
from dyno_cluster import DynoCluster

class DynoNode(Node):
    def __init__(self, ip, port=8102,
                 dnode_port=8101, data_store_port=1212):
        super(DynoNode, self).__init__(ip, port)
        self.name="Dyno" + self.name
        self.conf_file = None
        self.dnode_port = dnode_port
        self.data_store_node = RedisNode(ip, data_store_port)

    def get_connection(self):
        # should return the connection to the dyno port not the redis
        print("returning connection at %s:%d" % (self.ip, self.port))
        return redis.StrictRedis(self.ip, self.port, db=0)
