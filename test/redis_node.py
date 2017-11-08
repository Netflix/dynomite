#!/usr/bin/env python3
import redis
from node import Node

class RedisNode(Node):
    def __init__(self, host, ip, port):
        super(RedisNode, self).__init__(host, ip, port)
        self.name = "Redis" + self.name

    def get_connection(self):
        return redis.StrictRedis(self.host, self.port, db=0)
