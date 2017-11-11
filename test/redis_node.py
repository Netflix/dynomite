#!/usr/bin/env python3
import redis
from node import Node

class RedisNode(Node):
    def __init__(self, ip, port):
        super(RedisNode, self).__init__(ip, port)
        self.name = "Redis" + self.name

    def get_connection(self):
        return redis.StrictRedis(self.ip, self.port, db=0)
