#!/usr/bin/env python3
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
    print("Running %s tests" % test_name)
    for x in range(0, max_keys):
        key = create_key(test_name, x)
        c.run_verify("set", key, string_generator(size=random.randint(1, max_payload)))
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
    time.sleep(7)
    c.run_verify("exists", key)

def run_multikey_test(c, max_keys=1000, max_payload=10):
    #Set some
    test_name="MULTIKEY"
    print("Running %s tests" % test_name)
    for n in range(0, 100):
        kv_pairs = {}
        len = random.randint(1, 50)
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

def run_script_tests(c):
    TEST_NAME="SCRIPTS"
    print("Running %s tests" % TEST_NAME)

    # This script basically executes 'GET <key>'.
    SCRIPT_BODY='{}'.format("return redis.call('get', KEYS[1])")
    EXPECTED_VALUE = "value1"

    # Load a simple script.
    script_hash = c.run_verify("script_load", SCRIPT_BODY)

    # Make sure that the script exists.
    assert c.run_verify("script_exists", script_hash)[0] == True

    # Create a key to test with.
    key = create_key(TEST_NAME, "key1")
    c.run_verify("set", key, EXPECTED_VALUE)

    # Verify that the result of the script is the same in both Dynomite and Redis using
    # EVALSHA.
    evalsha_result = c.run_verify("evalsha", script_hash, 1, key)

    # Decode from UTF-8 before comparing the result.
    assert str(evalsha_result, 'utf-8') == EXPECTED_VALUE

    # Flush the Redis script cache through Dynomite.
    c.run_dynomite_only("script_flush")

    # Verify that the script no longer exists.
    assert c.run_dynomite_only("evalsha", script_hash, 1, key) == None


def run_hash_tests(c, max_keys=10, max_fields=1000):
    def create_key_field(keyid=None, fieldid=None):
        if keyid is None:
            keyid = random.randint(0, max_keys - 1)
        if fieldid is None:
            fieldid = random.randint(0, max_fields- 1)
        key = create_key(test_name, keyid)
        field = create_key("_field", fieldid)
        return (key, key + field)

    test_name="HASH_MAP"
    print("Running %s tests" % test_name)

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

    # hgetall, hkeys, hvals
    # We may get differently ordered results from both Redis and Dynomite, so instruct
    # client to sort all results before comparing.
    c.set_sort_before_compare(True)
    key, _ = create_key_field()
    c.run_verify("hgetall", key)
    key, _ = create_key_field()
    c.run_verify("hkeys", key)
    key, _ = create_key_field()
    c.run_verify("hvals", key)

    # TODO: Still have ordering issues with HSCAN. Find another way to test.
    # finally do a hscan
    #key, _ = create_key_field()
    #next_index = 0;
    #while True:
        #result = c.run_verify("hscan", key, next_index)
        #next_index = result[0]
        #print next_index
        #if next_index == 0:
            #break

def run_read_repair_test(c, num_keys=10):
    # Enable read repairs (TODO)

    test_name="READ_REPAIR_TEST"
    print("Running %s tests" % test_name)

    dyno_cluster = c.get_dynomite_cluster()

    # We need a connection to a DC that has RF > 1, since we're going to be using quorum.
    print("\t-Ensuring we have a connection to a multi-rack DC")
    c.ensure_underlying_dyno_conn_is_multi_dc()

    print("\t-Enabling read repairs")
    # Enable read repairs in the cluster
    dyno_cluster.enable_read_repairs()

    print("\t-Enabling DC_SAFE_QUORUM")
    # Make the consistency level of the cluster as "DC_SAFE_QUORUM"
    # as read repairs are disabled on "DC_ONE"
    # TODO: Remove when no longer true
    dyno_cluster.set_cluster_consistency_level("DC_SAFE_QUORUM")

    # Set a key
    key = create_key(test_name, "_1")
    value = string_generator(size=random.randint(1, 1024))

    print("\t-Writing to key '%s'" % key)
    c.run_verify("set", key, value)

    # 'get' and verify that the value is the same.
    assert value == str(c.run_verify("get", key), 'utf-8')

    print("\t-Simulating partial update to key '%s'" % key)
    # Below, we simulate a partial write by updating the value of 'key' on only
    # one replica, and updating its timestamp on that replica.
    # After simulating such a write, doing a quorum read will eventually fix the
    # key on all replicas.
    ADD_SET_MD_KEY = "._add-set"
    REPAIRED_VALUE = "REPAIRED_VALUE"

    # Find a DC with multiple racks (i.e. RF > 1)
    dc_name, racks = dyno_cluster.get_multi_rack_dc()
    num_racks = len(racks)

    # Pick any rack
    rack = next(iter(racks.keys()))

    # Find the exact node(shard) in the rack that has 'key'
    node_with_key = dyno_cluster.find_node_with_key(dc_name, rack, key)
    # Get the connection to the Redis process on that node.
    redis_conn = node_with_key.get_data_store_connection()
    assert redis_conn.exists(key) == True
    assert redis_conn.delete(key) == 1
    assert redis_conn.exists(key) == False

    # Make sure that we have the dynomite reserved metadata key
    assert redis_conn.exists(ADD_SET_MD_KEY) == True

    # Get the TS of our 'key' and update it by 1.
    add_ts = redis_conn.zscore(ADD_SET_MD_KEY, key) + 1.0
    # Update the TS of that key to make it appear that this node has
    # the latest value.
    # Note: Redis-py unfortunately has backward compatibility issues, so we change the API
    # based on the version.
    # https://github.com/andymccurdy/redis-py/issues/1068#issuecomment-439175760
    if redis.VERSION[0] < 3:
        redis_conn.zadd(ADD_SET_MD_KEY, add_ts, key)
    else:
        redis_conn.zadd(ADD_SET_MD_KEY, {key: add_ts})
    score = redis_conn.zscore(ADD_SET_MD_KEY, key)
    assert score == add_ts , score

    # Update with a value that we want to repair with
    assert redis_conn.set(key, REPAIRED_VALUE) == True

    test_success = False
    # Attempt to retrieve the up to 20 times and confirm that we read the
    # repaired value.
    # We retry multiple times since the quorum can be achieved with the majority replicas
    # that have the older data, but we'll eventually conflict with the replica with the
    # latest data causing the repair on all nodes.
    for i in range(0, 20):
        final_val = str(c.run_dynomite_only("get", key), 'utf-8')
        if final_val == REPAIRED_VALUE:
            test_success = True
            break

    assert test_success == True
    print("\t-Confirmed read repair took place successfully")
    print("\t-Disabling read repairs")
    dyno_cluster.disable_read_repairs()


def comparison_test(redis, dynomite, debug):
    c = dual_run(redis, dynomite, debug)
    run_key_value_tests(c)

    # XLarge payloads
    run_key_value_tests(c, max_keys=10, max_payload=5*1024*1024)

    run_multikey_test(c)
    run_hash_tests(c, max_keys=10, max_fields=100)
    run_script_tests(c)

    # Run read repairs tests last since we change the state of the cluster to use
    # DC_SAFE_QUORUM
    run_read_repair_test(c)
    print("All test ran fine")

def main(args):
    # This test assumes for now that the nodes are running at the given ports.
    # This is done by travis.sh. Please check that file and the corresponding
    # yml files for each dynomite instance there to get an idea of the topology.
    r = RedisNode(ip="127.0.1.1", port=1212)
    d1 = DynoNode(ip="127.0.1.2", data_store_port=22121)
    d2 = DynoNode(ip="127.0.1.3", data_store_port=22122)
    d3 = DynoNode(ip="127.0.1.4", data_store_port=22123)
    d4 = DynoNode(ip="127.0.1.5", data_store_port=22124)
    d5 = DynoNode(ip="127.0.1.6", data_store_port=22125)
    dyno_nodes = [d1,d2,d3,d4,d5]
    cluster = DynoCluster(dyno_nodes)
    try:
        comparison_test(r, cluster, args.debug)
    except ResultMismatchError as r:
        print(r)
        return 1
    return 0

if __name__ == "__main__":
    args = parse_args()
    sys.exit(main(args))
