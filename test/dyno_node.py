#!/usr/bin/env python3
import redis
from node import Node
from redis_node import RedisNode

from plumbum import BG
from plumbum import local

dynomite_bin = local.get('./test/_binaries/dynomite', 'src/dynomite')

class DynoNode(Node):
    #TODO: Decouple from Redis and make it more generic
    def __init__(self, spec, seeds_list):
        super(DynoNode, self).__init__(spec.ip, spec.client_port)
        self.name="Dyno" + self.name
        self.conf_file = None
        self.spec = spec
        self.seeds_list = seeds_list
        self.dnode_port = spec.dnode_port
        self.data_store_port = spec.data_store_port
        self.data_store_node = RedisNode(self.ip, spec.data_store_port)
        self.logfile = 'logs/dynomite_{}.log'.format(self.ip)
        self.proc_future = None

    def launch(self):
        # Write the configuration to a file according to 'self.spec'.
        config_filename = self.spec.write_config(self.seeds_list)

        # Launch the underlying data store process first.
        self.data_store_node.launch()

        # Launch the dynomite process
        self.proc_future = dynomite_bin['-o', self.logfile, '-c', config_filename,
            '-v6'] & BG(-9)

    def teardown(self):
        # First teardown the Dynomite node.
        self.proc_future.proc.kill()
        self.proc_future.wait()

        # Lastly, teardown the storage node.
        self.data_store_node.teardown()

    def __enter__(self):
        self.launch()

    def __exit__(self, type_, value, traceback):
        self.teardown()

    def get_dyno_node_pid(self):
        return self.proc_future.proc.pid

    def get_storage_node_pid(self):
        return self.data_store_node.get_pid()

    def get_data_store_connection(self):
        # should return the connection to the redis port
        return redis.StrictRedis(self.ip, self.data_store_port, db=0)

    def get_connection(self):
        # should return the connection to the dyno port not the redis
        print("returning connection at %s:%d" % (self.ip, self.port))
        return redis.StrictRedis(self.ip, self.port, db=0)
