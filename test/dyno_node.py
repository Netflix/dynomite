#!/usr/bin/python
import redis
from node import Node
from redis_node import RedisNode
from dyno_cluster import DynoCluster

class DynoNode(Node):
    def __init__(self, host="localhost", ip="127.0.0.1", port=8102,
                 dnode_port=8101, data_store_port=22122):
        super(DynoNode, self).__init__(host, ip, port)
        self.name="Dyno" + self.name
        self.conf_file = None
        self.dnode_port = dnode_port
        self.data_store_node = RedisNode(host, ip, data_store_port)

    def get_connection(self):
        # should return the connection to the dyno port not the redis
        print("returning connection at %s:%d" % (self.host, self.port))
        return redis.StrictRedis(self.host, self.port, db=0)
