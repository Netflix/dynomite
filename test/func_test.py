#!/usr/bin/python
import redis
import argparse
import random
import string 
import sys
import time
from utils import string_generator, number_generator
from dyno_node import DynoNode
from redis_node import RedisNode
from dyno_cluster import DynoCluster
from dual_run import dual_run, ResultMismatchError

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true')
    return parser.parse_args()


def create_key(test_name, key_id):
    return test_name + "_" + str(key_id)

def run_key_value_tests(c, max_keys=1000, max_payload=1024):
    #Set some
    test_name="KEY_VALUE"
    print "Running %s tests" % test_name
    for x in range(0, max_keys):
        key = create_key(test_name, x)
        value = string_generator(size=random.randint(512, 1024))
        c.run_verify("set", key, value)
    # get them and see
    for x in range(0, max_keys):
        key = create_key(test_name, x)
        c.run_verify("get", key)
    # append a key
    key = create_key(test_name, random.randint(0, max_keys-1))
    value = string_generator()
    c.run_verify("append", key, value)
    c.run_verify("get", key)
    # expire a few
    key = create_key(test_name, random.randint(0, max_keys-1))
    c.run_verify("expire", key, 5)
    time.sleep(7);
    c.run_verify("exists", key)

def run_multikey_test(c, max_keys=1000, max_payload=10):
    #Set some
    test_name="MULTIKEY"
    print "Running %s tests" % test_name
    for n in range(0, 100):
        kv_pairs = {}
        len = random.randint(1, 5)
        for x in range(0, len):
            key_id = random.randint(0, max_keys-1)
            key = create_key(test_name, key_id)
            value = string_generator(size=random.randint(1, max_payload))
            kv_pairs[key] = value
        c.run_verify("mset", kv_pairs)
        keys = []
        len = random.randint(1, 50)
        for x in range(0, len):
            key_id = random.randint(0, max_keys-1)
            key = create_key(test_name, key_id)
            keys.append(key)
        c.run_verify("mget", keys)

def run_hash_tests(c, max_keys=10, max_fields=1000):
    def create_key_field(keyid=None, fieldid=None):
        if keyid is None:
            keyid = random.randint(0, max_keys - 1)
        if fieldid is None:
            fieldid = random.randint(0, max_fields- 1)
        key = create_key(test_name, keyid);
        field = create_key("_field", fieldid);
        return (key, key + field)

    test_name="HASH_MAP"
    print "Running %s tests" % test_name

    #hset
    for key_iter in range(0, max_keys):
        for field_iter in range(0, max_fields):
            key, field = create_key_field(key_iter, field_iter)
            value = number_generator()
            c.run_verify("hset", key, field, value)

    # hmset
    keyid = random.randint(0, max_keys-1)
    key, _ = create_key_field(keyid)
    kv_pairs = {}
    for x in range(0, 50):
        _, field = create_key_field(keyid)
        value = number_generator()
        kv_pairs[field] = value
    c.run_verify("hmset", key, kv_pairs)

    # hmget
    keyid = random.randint(0, max_keys-1)
    key, _ = create_key_field(keyid)
    list_args = [key]
    for x in range(0, 5):
        _, field = create_key_field(keyid)
        list_args.append(field)
    args = tuple(list_args)
    c.run_verify("hmget", *args)

    # hincrby, hdel, hexists
    key, field = create_key_field()
    c.run_verify("hincrby", key, field, 50)
    c.run_verify("hdel", key, field)
    c.run_verify("hexists", key, field)
    key, _ = create_key_field()
    c.run_verify("hlen", key)

    # These have issues because redis instances can return different values.
    # hgetall, hkeys, hvals
    #key, _ = create_key_field()
    #c.run_verify("hgetall", key)
    #key, _ = create_key_field()
    #c.run_verify("hkeys", key)
    #key, _ = create_key_field()
    #c.run_verify("hvals", key)

    # finally do a hscan
    #key, _ = create_key_field()
    #next_index = 0;
    #while True:
        #result = c.run_verify("hscan", key, next_index)
        #next_index = result[0] 
        #print next_index
        #if next_index == 0:
            #break

def main(args):
    # This test assumes for now that the nodes are running at the given ports.
    # This is done by travis.sh. Please check that file and the corresponding
    # yml files for each dynomite instance there to get an idea of the topology.
    r = RedisNode(host="localhost", ip="127.0.0.1", port=1212)
    d1 = DynoNode(host="127.0.0.1", ip="127.0.0.1", data_store_port=22121)
    d2 = DynoNode(host="127.0.0.2", ip="127.0.0.2", data_store_port=22122)
    d3 = DynoNode(host="127.0.0.3", ip="127.0.0.3", data_store_port=22123)
    d4 = DynoNode(host="127.0.0.4", ip="127.0.0.4", data_store_port=22124)
    d5 = DynoNode(host="127.0.0.5", ip="127.0.0.5", data_store_port=22125)
    dyno_nodes = [d1,d2,d3,d4,d5]
    cluster = DynoCluster(dyno_nodes)
    r_c = r.get_connection()
    d_c = cluster.get_connection()
    c = dual_run(r_c, d_c, args.debug)
    try:
        run_key_value_tests(c)
        run_key_value_tests(c, max_payload=16384*1024)
        run_hash_tests(c, max_keys=10, max_fields=100)
        run_multikey_test(c)
        print "All test ran fine"
    except ResultMismatchError as r:
        print r;
        return 1
    return 0

if __name__ == "__main__":
    args = parse_args()
    sys.exit(main(args))
