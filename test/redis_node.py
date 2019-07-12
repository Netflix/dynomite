#!/usr/bin/env python3
import redis
from node import Node

from plumbum import BG
from plumbum import local

redis_bin = local.get('./test/_binaries/redis-server', 'redis-server')

class RedisNode(Node):
    def __init__(self, ip, port):
        super(RedisNode, self).__init__(ip, port)
        self.name = "Redis" + self.name
        self.logfile = 'logs/redis_{}.log'.format(self.ip)
        self.proc_future = None

    def get_connection(self):
        return redis.Redis(self.ip, self.port, db=0)

    def get_pid(self):
        return self.proc_future.proc.pid

    def launch(self):
        self.proc_future = \
            (redis_bin['--bind', self.ip, '--port', self.port, '--loglevel', 'verbose'] > self.logfile) & BG(-9)

    def teardown(self):
        self.proc_future.proc.kill()
        self.proc_future.wait()

    def __enter__(self):
        self.launch()

    def __exit__(self, type_, value, traceback):
        self.teardown()
