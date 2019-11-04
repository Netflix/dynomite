/*
* Dynomite - A thin, distributed replication layer for multi non-distributed
* storages. Copyright (C) 2019 Netflix, Inc.
*/

#include <ctype.h>
#include <stdio.h>

#include "../dyn_core.h"
#include "../dyn_dnode_peer.h"
#include "../dyn_util.h"
#include "dyn_proto.h"
#include "dyn_proto_repair.h"


struct cmd_info proto_cmd_info[] = {
 {"UNKNOWN", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},

 // Begin memcache commands. All Memcache commands are not supported. (idx 1 onwards)
 {"MC_GET", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"MC_GETS", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"MC_DELETE", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"MC_CAS", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"MC_SET", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"MC_ADD", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"MC_REPLACE", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"MC_APPEND", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"MC_PREPEND", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"MC_INCR", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"MC_DECR", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"MC_TOUCH", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"MC_QUIT", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"MC_NUM", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"MC_STORED", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"MC_NOT_STORED", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"MC_EXISTS", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"MC_NOT_FOUND", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"MC_END", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"MC_VALUE", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"MC_DELETED", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"MC_TOUCHED", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"MC_ERROR", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"MC_CLIENT_ERROR", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"MC_SERVER_ERROR", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},

 // Begin Redis commands. They are partially supported. (idx 26 onwards)
 {"DEL", 0, 1, 0, 0, 0, true, true, false, 0, 0, -1, 0, -1, 0, DEL_SCRIPT, 0, 0},
 {"EXISTS", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"EXPIRE", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"EXPIREAT", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"PEXPIRE", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"PEXPIREAT", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"PERSIST", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"PTTL", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"SCAN", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"SORT", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"TTL", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"TYPE", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"APPEND", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"BITCOUNT", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"BITPOS", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"DECR", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"DECRBY", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"DUMP", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"GET", 0, 1, 0, 0, 0, true, false, true, 0, 0, -1, 0, -1, 0, GET_SCRIPT,
     MSG_REQ_REDIS_SET, MSG_REQ_REDIS_DEL},
 {"GETBIT", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"GETRANGE", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"GETSET", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"INCR", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"INCRBY", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"INCRBYFLOAT", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"MSET", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"MSET", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"MGET", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"PSETEX", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"SET", 1, 1, 0, 1, 0, true, false, false, 0, 0, -1, 0, 1, 0, SET_SCRIPT, 0, 0}, // idx 55
 {"SETBIT", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"SETEX", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"SETNX", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"SETRANGE", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"STRLEN", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"HDEL", 1, 1, 1, 0, 0, true, true, false, 0, 0, 1, 1, -1, 0, HDEL_SCRIPT, 0, 0}, // idx 61
 {"HEXISTS", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"HGET", 1, 1, 1, 0, 0, true, false, true, 0, 0, 1, 0, -1, 0, HGET_SCRIPT,
     MSG_REQ_REDIS_HSET, MSG_REQ_REDIS_HDEL}, // idx 63
 {"HGETALL", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"HINCRBY", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"HINCRBYFLOAT", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"HKEYS", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"HLEN", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"HMGET", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"HMSET", 2, 1, 1, 1, 0, true, false, false, 0, 0, 1, 2, 2, 2, HSET_SCRIPT, 0, 0}, // idx 70
 {"HSET", 2, 1, 1, 1, 0, true, false, false, 0, 0, 1, 0, 2, 0, HSET_SCRIPT, 0, 0}, // idx 71
 {"HSETNX", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"HSCAN", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"HVALS", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"HSTRLEN", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"KEYS", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"INFO", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"LINDEX", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"LINSERT", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"LLEN", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"LPOP", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"LPUSH", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"LPUSHX", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"LRANGE", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"LREM", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"LSET", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"LTRIM", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"PING", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"QUIT", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"RPOP", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"RPOPLPUSH", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"RPUSH", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"RPUSHX", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"SADD", 1, 1, 1, 0, 0, true, false, false, 0, 0, 1, 1, -1, 0, SADD_SCRIPT, 0, 0}, // idx 94
 {"SCARD", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"SDIFF", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"SDIFFSTORE", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"SINTER", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"SINTERSTORE", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"SISMEMBER", 1, 1, 1, 0, 0, true, false, true, 0, 0, 1, 0, -1, 0, HGET_SCRIPT,
     MSG_REQ_REDIS_SADD, MSG_REQ_REDIS_SREM}, // idx 100
 {"SLAVEOF", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"SMEMBERS", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"SMOVE", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"SPOP", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"SRANDMEMBER", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"SREM", 1, 1, 1, 0, 0, true, true, false, 0, 0, 1, 1, -1, 0, HDEL_SCRIPT, 0, 0}, // idx 106
 {"SUNION", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"SUNIONSTORE", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"SSCAN", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"ZADD", 2, 1, 1, 1, 1, true, false, false, 0, 0, 2, 2, 1, 2, ZADD_SCRIPT, 0, 0}, // idx 110
 {"ZCARD", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"ZCOUNT", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"ZINCRBY", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"ZINTERSTORE", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"ZLEXCOUNT", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"ZRANGE", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"ZRANGEBYLEX", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"ZRANGEBYSCORE", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"ZRANK", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"ZREM", 1, 1, 1, 0, 0, true, true, false, 0, 0, 1, 1, -1, 0, HDEL_SCRIPT, 0, 0}, // idx 120
 {"ZREMRANGEBYRANK", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"ZREMRANGEBYLEX", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"ZREMRANGEBYSCORE", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"ZREVRANGE", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"ZREVRANGEBYLEX", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"ZREVRANGEBYSCORE", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"ZREVRANK", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"ZSCORE", 1, 1, 1, 0, 0, true, false, true, 0, 0, 1, 0, -1, 0, HGET_SCRIPT,
     MSG_REQ_REDIS_ZADD, MSG_REQ_REDIS_ZREM}, // idx 128
 {"ZUNIONSTORE", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"ZSCAN", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"EVAL", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"EVALSHA", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"GEOADD", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"GEORADIUS", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"GEODIST", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"GEOHASH", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"GEOPOS", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"GEORADIOUSBYMEMBER", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"UNLINK", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"JSONSET", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"JSONGET", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"JSONDEL", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"JSONTYPE", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"JSONMGET", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"JSONARRAPPEND", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"JSONARRINSERT", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"JSONARRLEN", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"JSONOBJKEYS", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"JSONOBJLEN", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"CONFIG", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"SCRIPT", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"SCRIPT_LOAD", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"SCRIPT_EXISTS", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"SCRIPT_FLUSH", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
 {"SCRIPT_KILL", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0}

 // Ignore RSP msg types.
};


static int total_tokens_of_type(int variadic_jump, int start_pos, int min_num_tokens,
    int nelem) {

  if (variadic_jump == 0) return min_num_tokens;

  int total = (nelem - start_pos) / variadic_jump;
  total += (nelem - start_pos) % variadic_jump;
  return total;
}

static rstatus_t parse_tokens_of_type(int num_tokens, int out_token_idx,
    int variadic_jump, int start_pos, struct array *source_info,
    bool is_key, struct array **out) {
  if (num_tokens == 0) return DN_OK;

  if (*out == NULL) {
    *out = array_create(num_tokens, sizeof(struct argpos));
    if (*out == NULL) return DN_ENOMEM;
  }

  // If the variadic jump is 0 or 1, we need to jump by 1 in case num_tokens > 0. Else we
  // jump by the variadic jump position itself.
  int jump_by = (variadic_jump == 0 || variadic_jump == 1) ? 1 : variadic_jump;

  for (; num_tokens > 0; -- num_tokens) {
    if (!is_key) {
      struct argpos *token_pos = (struct argpos*)array_push(*out);
      struct argpos *orig_token_pos =
          (struct argpos*)array_get(source_info, start_pos - 1 + out_token_idx);
      token_pos->start = orig_token_pos->start;
      token_pos->end = orig_token_pos->end;
      out_token_idx += jump_by;
    } else {
      // TODO: Make more concise.
      struct keypos *token_pos = (struct keypos*)array_push(*out);
      struct keypos *orig_token_pos =
          (struct keypos*)array_get(source_info, start_pos - 1 + out_token_idx);
      token_pos->start = orig_token_pos->start;
      token_pos->end = orig_token_pos->end;
      token_pos->tag_start = orig_token_pos->tag_start;
      token_pos->tag_end = orig_token_pos->tag_end;
      out_token_idx += jump_by;
    }
  }

  return DN_OK;
}

static rstatus_t get_values_from_source(int num_tokens, int start_pos,
    struct array *source_info, struct array **out_values) {
  *out_values = array_create(num_tokens, sizeof(struct argpos));
  if (*out_values == NULL) return DN_ENOMEM;

  int i;
  for (i = 0; i < num_tokens; ++i) {
    struct argpos *value_pos = (struct argpos*)array_push(*out_values);
    struct argpos *orig_value_pos = (struct argpos*)array_get(source_info, start_pos++);
    value_pos->start = orig_value_pos->start;
    value_pos->end = orig_value_pos->end;
  }

  return DN_OK;
}

rstatus_t post_parse_optional_args(struct msg *orig_msg) {
  struct write_with_ts *out_struct = &orig_msg->msg_info;
  struct cmd_info *orig_cmd_info = &proto_cmd_info[orig_msg->type];

  switch (orig_msg->type) {
    case MSG_REQ_REDIS_ZADD: ;
      int idx = 0;
      int num_args = array_n(orig_msg->args);

      // All the optional fields for ZADD come before the first score, so record all the options
      // provided.
      bool nx = false;
      bool xx = false;
      bool ch = false;
      bool incr = false;
      while (idx < num_args) {
        struct argpos *opt_arg = (struct argpos*)array_get(orig_msg->args, idx);
        if (opt_arg == NULL) return DN_ERROR;

        uint8_t *opt_arg_start = opt_arg->start;
        if (dn_strcasecmp(opt_arg_start, "NX") == 0) {
          nx = true;
        } else if (dn_strcasecmp(opt_arg_start, "XX") == 0) {
          xx = true;
        } else if (dn_strcasecmp(opt_arg_start, "INCR") == 0) {
          incr = true;
        } else if (dn_strcasecmp(opt_arg_start, "CH") == 0) {
          ch = true;
        } else {
          break;
        }
        ++idx;

        if (out_struct->optionals == NULL) {
          out_struct->optionals = array_create(1, sizeof(struct argpos));
          if (out_struct->optionals == NULL) return DN_ENOMEM;
        }
        struct argpos *record_opt_pos = (struct argpos*)array_push(out_struct->optionals);
        record_opt_pos->start = opt_arg->start;
        record_opt_pos->end = opt_arg->end;
      }

      // Both these options cannot be set together.
      if (nx && xx) return DN_ERROR;

      out_struct->num_optionals = idx;
      break;
    default:
      return DN_OK;
  }
  return DN_OK;
}

/*
 * Parses the original 'struct msg' and fills in 'orig_msg->msg_info' with all
 * the keys, fields, values and optional fields.
 *
 * TODO: Consider getting rid of this function if we move to a per-command parser model.
 *
 */
rstatus_t post_parse_msg(struct msg *orig_msg) {

  struct write_with_ts *out_struct = &orig_msg->msg_info;
  struct cmd_info *orig_cmd_info = &proto_cmd_info[orig_msg->type];
  ASSERT(out_struct != NULL);

  out_struct->cmd_type = orig_msg->type;
  out_struct->ts = orig_msg->timestamp;
  out_struct->num_fields = orig_cmd_info->min_num_fields;
  out_struct->num_values = orig_cmd_info->min_num_values;
  out_struct->num_optionals = 0;
  out_struct->keys = NULL;
  out_struct->fields = NULL;
  out_struct->values = NULL;

  {
    int start_from_arg_pos; 
    // If the first key position is '0', then it wouldn't be found in the 'args'
    // array and we need to account for it from the 'keys' array.
    if (orig_cmd_info->first_key_pos == 0) {
      start_from_arg_pos = 0;

      // TODO: Refactor
      if (orig_cmd_info->variadic_key_jump > 0) {
        out_struct->num_keys = 1; // Counting the one in 'orig_msg->keys'.
      } else {
        out_struct->num_keys = 0;
      }
    } else {
      start_from_arg_pos = orig_cmd_info->first_key_pos - 1;
      out_struct->num_keys = 0;
    }
    out_struct->num_keys += total_tokens_of_type(
        orig_cmd_info->variadic_key_jump,
        start_from_arg_pos,
        orig_cmd_info->min_num_keys,
        orig_msg->args->nelem);
  }

  if (out_struct->num_keys > 0) {
    // TODO: Make more concise.
    out_struct->keys = array_create(out_struct->num_keys, sizeof(struct keypos));
    if (out_struct->keys == NULL) goto error;

    int key_idx = 0;
    if (orig_cmd_info->first_key_pos == 0) {
      struct keypos *kpos = (struct keypos*)array_push(out_struct->keys);
      struct keypos *orig_kpos = (struct keypos*)array_get(orig_msg->keys, 0);
      kpos->start = orig_kpos->start;
      kpos->end = orig_kpos->end;
      kpos->tag_start = orig_kpos->tag_start;
      kpos->tag_end = orig_kpos->tag_end;
      ++key_idx;
    }

    if (parse_tokens_of_type(out_struct->num_keys - key_idx, key_idx,
        orig_cmd_info->variadic_key_jump, orig_cmd_info->first_key_pos,
        orig_msg->args, true, &out_struct->keys) != DN_OK) {
      goto error;
    }
  }

  // Check if this command has optional arguments and parse + validate them.
  if (orig_cmd_info->num_optionals > 0) {
    post_parse_optional_args(orig_msg);
  }

  // If we have optional fields, we need to offset the field and value positions
  // accordingly.
  int first_field_pos = orig_cmd_info->first_field_pos
      + out_struct->num_optionals;
  int first_value_pos = orig_cmd_info->first_value_pos
      + out_struct->num_optionals;

  out_struct->num_fields = total_tokens_of_type(
      orig_cmd_info->variadic_field_jump,
      first_field_pos - 1 /* start_pos */,
      orig_cmd_info->min_num_fields,
      orig_msg->args->nelem);

  if (parse_tokens_of_type(out_struct->num_fields, 0,
          orig_cmd_info->variadic_field_jump, first_field_pos,
          orig_msg->args, false, &out_struct->fields) != DN_OK) {
    goto error;
  }

  out_struct->num_values = total_tokens_of_type(
      orig_cmd_info->variadic_value_jump,
      first_value_pos - 1/* start_pos */,
      orig_cmd_info->min_num_values,
      orig_msg->args->nelem);

  if (parse_tokens_of_type(out_struct->num_values, 0,
          orig_cmd_info->variadic_value_jump, first_value_pos,
          orig_msg->args, false, &out_struct->values) != DN_OK) {
    goto error;
  }

  // Swap the add and remove sets if this is a delete command.
  if (orig_cmd_info->is_delete) {
    out_struct->add_set = REM_SET_STR;
    out_struct->rem_set = ADD_SET_STR;
  } else {
    out_struct->add_set = ADD_SET_STR;
    out_struct->rem_set = REM_SET_STR;
  }
 
  out_struct->rewrite_script = orig_cmd_info->rewrite_script;
  return DN_OK;
error:

  // Destroy allocated arrays.
  array_destroy(out_struct->keys);
  array_destroy(out_struct->fields);
  array_destroy(out_struct->values);
  return DN_ERROR;
}

/*
 * Populates a 'write_with_ts' structure based on the information contained in
 * 'most_updated_rsp'.
 *
 */
rstatus_t obtain_info_from_latest_rsp(struct response_mgr *rspmgr,
    struct msg *most_updated_rsp, bool repair_by_add,
    struct write_with_ts *repair_msg_info) {

  msg_type_t orig_msg_type = rspmgr->msg->orig_type;
  msg_type_t repair_msg_type = (repair_by_add == true) ?
                                proto_cmd_info[orig_msg_type].repair_by_add :
                                proto_cmd_info[orig_msg_type].repair_by_rem;

  struct write_with_ts *orig_msg_info = &rspmgr->msg->orig_msg->msg_info;

  // Copy all the relevant information from the 'most_updated_rsp' to craft a
  // 'struct write_with_ts' to be used while creating the repair msg.
  repair_msg_info->cmd_type = repair_msg_type;
  repair_msg_info->rewrite_script = proto_cmd_info[repair_msg_type].rewrite_script;
  repair_msg_info->ts = most_updated_rsp->timestamp;

  repair_msg_info->num_keys = orig_msg_info->num_keys;
  repair_msg_info->num_fields = orig_msg_info->num_fields;
  repair_msg_info->num_values = orig_msg_info->num_values;
  repair_msg_info->num_optionals = 0;

  repair_msg_info->keys = orig_msg_info->keys;
  repair_msg_info->fields = orig_msg_info->fields;

  if (repair_by_add) {
    repair_msg_info->add_set = ADD_SET_STR;
    repair_msg_info->rem_set = REM_SET_STR;
  } else {
    repair_msg_info->add_set = REM_SET_STR;
    repair_msg_info->rem_set = ADD_SET_STR;
  }

  repair_msg_info->total_num_tokens = orig_msg_info->total_num_tokens;

  // TODO: When we support multi value gets, we should update this logic. This will
  // work only for a single value get since we're only checking a single status.
  // (Eg: GET, HGET vs. SMEMBERS)
  struct argpos* status_pos = (struct argpos*)array_get(most_updated_rsp->args, 0);
  if (*status_pos->start == 'E') {
    if (repair_msg_info->num_fields) {
      if (get_values_from_source(repair_msg_info->num_fields,
          repair_msg_info->num_fields * 2 /* start_pos */,
          most_updated_rsp->args, &repair_msg_info->values) != DN_OK) {
        goto error;
      }
      repair_msg_info->num_values += repair_msg_info->num_fields;
      repair_msg_info->total_num_tokens += repair_msg_info->num_fields;
    } else {
      if (get_values_from_source(repair_msg_info->num_keys,
          repair_msg_info->num_keys * 2 /* start_pos */,
          most_updated_rsp->args, &repair_msg_info->values) != DN_OK) {
        goto error;
      }
      repair_msg_info->num_values += repair_msg_info->num_keys;
      repair_msg_info->total_num_tokens += repair_msg_info->num_keys;
    }
  }
  return DN_OK;

 error:
  array_destroy(repair_msg_info->values);
  return DN_ERROR;
}

#define REDIS_PRTCL_BEGIN_TOTAL_TOKENS "*%d\r\n"
#define REDIS_PRTCL_INT_ARG_FMT "$%d\r\n%d\r\n"
#define REDIS_PRTCL_VARCHAR_ARG_FMT "$%d\r\n%.*s\r\n"
#define REDIS_PRTCL_LLU_ARG_FMT "$%d\r\n%llu\r\n"

/*
 * Helper function to update the total number of tokens in 'src' based on
 * information already present in the struct.
 *
 */
void update_total_num_tokens(struct write_with_ts *src) {
  int num_keys = src->num_keys;
  int num_fields = src->num_fields;
  int num_values = src->num_values;
  int num_optionals = src->num_optionals;

  // Add 2 by default, one for the 'EVAL' command and one for the script itself.
  src->total_num_tokens = 2;

  // Add one token for the total number of keys.
  ++src->total_num_tokens;

  // Add all the keys touched in the query.
  src->total_num_tokens += num_keys;

  // Add the add-set and rem-set keys.
  src->total_num_tokens += 2;

  // Add the command string. (Eg: SET, HSET, etc.)
  // Add the number of fields.
  // Add the timestamp.
  src->total_num_tokens += 3;

  if (proto_cmd_info[src->cmd_type].num_optionals > 0) {
    // If this command supports optional fields, add one token for the number of optional args.
    src->total_num_tokens += 1;
    // Then add the number of tokens for the optional fields themselves.
    src->total_num_tokens += num_optionals;
  }

  if (num_fields > 0 || num_values > 0) {

    // If we have both fields and values present, each field and value must be present in
    // pairs (since all Redis commands follow that protocol), else we list all the fields
    // or all the values.
    src->total_num_tokens += (num_fields > 0 && num_values > 0) ? num_fields * 2 :
         ((num_fields > 0) ? num_fields : num_values);
  }

}

/*
 * Using the information found in 'write_with_ts', this function populates 'msg'
 * with the entire script and supporting arguments based on the Redis wire protocol.
 *
 * It also updates 'src' with the total number of tokens.
 *
 * The format is:
 * <total_num_tokens> <script> <args>
 *
 * where <args> can be elaborated more into:
 * <key1>..(<keyN>) <+set> <-set> <orig_cmd> (<num_opts>) <num_flds> \
 * <ts> (<opt1>) .. (<optN>) (<fld1>) (<val1>) (<fldN>) ..
 *
 * Tokens shown above with parantheses are optional.
 *
 */
static rstatus_t create_redis_prtcl_script(struct write_with_ts *src,
    struct msg **msg_ptr) {

  int i;
  int num_keys = src->num_keys;
  int num_fields = src->num_fields;
  int num_values = src->num_values;
  int num_optionals = src->num_optionals;
  struct msg *msg = *msg_ptr;

  // Add the total number of tokens.
  THROW_STATUS(msg_append_format(msg, REDIS_PRTCL_BEGIN_TOTAL_TOKENS, 1,
      src->total_num_tokens));

  // Append the rewrite script.
  THROW_STATUS(msg_append_format(msg, "%s", 1, src->rewrite_script));

  // Add the total number of keys in the command. We add 2 for the add and remove sets.
  THROW_STATUS(msg_append_format(msg, REDIS_PRTCL_INT_ARG_FMT, 2,
      count_digits(num_keys + 2), num_keys + 2));

  // Add all the keys touched in the query.
  for (i = 0; i < src->num_keys; ++i) {
    struct keypos *elem = array_get(src->keys, i);
    uint32_t elem_len = keypos_elem_len(elem);
    THROW_STATUS(msg_append_format(msg, REDIS_PRTCL_VARCHAR_ARG_FMT, 3, elem_len,
        elem_len, elem->tag_start));
  }
  // Add the add-set and rem-set keys.
  int add_set_len = strlen(src->add_set);
  int rem_set_len = strlen(src->rem_set);
  THROW_STATUS(msg_append_format(msg, REDIS_PRTCL_VARCHAR_ARG_FMT, 3, add_set_len,
      add_set_len, src->add_set));
  THROW_STATUS(msg_append_format(msg, REDIS_PRTCL_VARCHAR_ARG_FMT, 3, rem_set_len,
      rem_set_len, src->rem_set));

  // Add the command string. (Eg: SET, HSET, etc.)
  char *orig_cmd_str = proto_cmd_info[src->cmd_type].cmd_str;
  THROW_STATUS(msg_append_format(msg, REDIS_PRTCL_VARCHAR_ARG_FMT, 3, strlen(orig_cmd_str),
      strlen(orig_cmd_str), orig_cmd_str));

  struct cmd_info *orig_cmd_info = &proto_cmd_info[src->cmd_type];
  // Add number of optionals if the command supports any. If the command does, the script will
  // expect a number of 0 or more.
  if (orig_cmd_info->num_optionals > 0) {
    THROW_STATUS(msg_append_format(msg, REDIS_PRTCL_INT_ARG_FMT, 2,
        count_digits(num_optionals), num_optionals));
  }

  // Add the number of fields.
  THROW_STATUS(msg_append_format(msg, REDIS_PRTCL_INT_ARG_FMT, 2,
      count_digits(num_fields), num_fields));

  // Add the timestamp.
  THROW_STATUS(msg_append_format(msg, REDIS_PRTCL_LLU_ARG_FMT, 2,
      count_digits(src->ts), src->ts));

  // Add the optional elements.
  if (num_optionals > 0) {
    for (i = 0; i < num_optionals; ++i) {
      struct argpos *opt_elem = array_get(src->optionals, i);
      uint32_t opt_len = argpos_elem_len(opt_elem);
      THROW_STATUS(msg_append_format(msg, REDIS_PRTCL_VARCHAR_ARG_FMT, 3, opt_len,
            opt_len, opt_elem->start));
    }
  }

  if (num_fields > 0 || num_values > 0) {

    // If we have both fields and values present, each field and value must be present in
    // pairs (since all Redis commands follow that protocol), else we list all the fields
    // or all the values.
    int total_iterations = (num_fields > 0 && num_values > 0) ? num_fields :
        ((num_fields > 0) ? num_fields : num_values);

    for (i = 0; i < total_iterations; ++i) {
      if (num_fields > 0) {
        struct argpos *field_elem = array_get(src->fields, i);
        uint32_t field_len = argpos_elem_len(field_elem);
        THROW_STATUS(msg_append_format(msg, REDIS_PRTCL_VARCHAR_ARG_FMT, 3, field_len,
            field_len, field_elem->start));
      }

      if (num_values > 0) {
        struct argpos *value_elem = array_get(src->values, i);
        uint32_t value_len = argpos_elem_len(value_elem);
        THROW_STATUS(msg_append_format(msg, REDIS_PRTCL_VARCHAR_ARG_FMT, 3, value_len,
            value_len, value_elem->start));
      }
    }
  }

  return DN_OK;
}

/*
 * Uses 'msg_info' and 'arg_str' to create a repair msg and parse it so that its ready
 * to be understood by Redis.
 *
 */
static rstatus_t finalize_repair_msg(struct context *ctx, struct conn *conn,
    struct write_with_ts *msg_info, struct msg **new_msg_ptr) {

  rstatus_t ret_status;
  struct msg *new_msg = NULL;
  new_msg = msg_get(conn, true, __FUNCTION__);
  if (new_msg == NULL) {
    ret_status = DN_ENOMEM;
    goto error;
  }

  ret_status = create_redis_prtcl_script(msg_info, &new_msg);
  if (ret_status != DN_OK) goto error;

  {
    // Set the 'pos' of the 'new_msg' so that the parser knows where to begin parsing from
    struct mbuf *new_mbuf = STAILQ_FIRST(&new_msg->mhdr);
    new_msg->pos = new_mbuf->pos;
  }

  // Parse the newly formed repair msg.
  new_msg->parser(new_msg, ctx);

  if (new_msg->result != MSG_PARSE_OK) {
    ret_status = DN_ERROR;
    goto error;
  }

  *new_msg_ptr = new_msg;
  return ret_status;
 error:
  if (new_msg != NULL) msg_put(new_msg);
  return ret_status;
}

/*
 * Finds all the timetamps from responses in rspmgr, updates the respective 'msg'
 * structures with their timestamp, and finds the response that has the latest
 * timestamp.
 *
 * Returns the response with the largest timestamp if it exists. 'repair_by_add' is
 * set according to whether the latest value for that key exists or not.
 *
 * Returns NULL if all the timestamps are the same.
 *
 */
static struct msg* find_most_updated_rsp(struct response_mgr *rspmgr,
    bool *repair_by_add) {
  int i;
  uint64_t biggest_ts = 0;
  struct msg *most_updated_rsp = NULL;
  bool at_least_one_repair = false;
  for (i = 0; i < rspmgr->good_responses; ++i) {
    struct msg *cur_rsp = rspmgr->responses[i];

    // Find the status of the key.
    struct argpos *key_status_arg = (struct argpos*)array_get(cur_rsp->args, 0);
    uint8_t *key_status = key_status_arg->start;
    if (*key_status == 'X') {  // Key does not exist
      continue;
    }

    struct argpos *ts_arg = (struct argpos*)array_get(cur_rsp->args, 1);
    uint8_t *j;
    cur_rsp->timestamp = 0;

    // Find the timestamp from the response buffer and tag the 'struct msg' with it.
    for (j = ts_arg->start; j < ts_arg->end; ++j) {
      char digit_ch = *j;
      ASSERT(isdigit(digit_ch));
      cur_rsp->timestamp = cur_rsp->timestamp * 10 + (uint64_t)(digit_ch - '0');
    }

    if (cur_rsp->timestamp > biggest_ts) {
      if (most_updated_rsp != NULL) {
        most_updated_rsp->needs_repair = true;
        at_least_one_repair = true;
      }
      biggest_ts = cur_rsp->timestamp;
      most_updated_rsp = cur_rsp;
      *repair_by_add = (*key_status == 'E');
    } else if (cur_rsp->timestamp < biggest_ts) {
      cur_rsp->needs_repair = true;
      at_least_one_repair = true;
    }
  }

  // If no one needs a repair, return NULL.
  return (at_least_one_repair) ? most_updated_rsp : NULL;
}

/*
 * The response buffers are returned in an internal Dynomite format which the
 * client should not see. This function adjusts the response buffer of the most
 * updated response (based on timestamp) to what the client would expect to see.
 *
 */
static void adjust_rsp_buffers_for_client(struct response_mgr *rspmgr,
    uint32_t num_values) {
  ;int num_value_digits = count_digits(num_values);

  // All responses begin with <status> & <ts>, so the first value, if present, will be at
  // position 2 in the args array of all responses.
  int start_idx_from_rsp = 2;

  int i;
  for (i = 0; i <rspmgr->good_responses; ++i) {
    struct msg *cur_rsp = rspmgr->responses[i];
    // If it's an outdated response, we won't show it to the client anyway, so don't waste
    // time adjusting its buffer.
    if (cur_rsp->needs_repair == true) continue;

    struct mbuf *rsp_mbuf = STAILQ_FIRST(&cur_rsp->mhdr);

    struct argpos* status_pos = (struct argpos*)array_get(cur_rsp->args, 0);
    if (*status_pos->start == 'X') {
      // If the value does not exist, we will get back a buffer of the format:
      // "*3\r\n$1\r\nX\r\n:0\r\n:0\r\n", so we just move the pointer to point to
      // ":0"
      rsp_mbuf->pos = status_pos->start + 7;
      continue;
    }

    // Update 'pos' pointer so that the metadata is not returned back to the client.
    if (*status_pos->start == 'R') {
      // If the item does not exist, the 2nd position in the buffer will be -1,
      // which is interpreted as "(nil)" by the client, so we just point the response
      // buffer to that.
      struct argpos* ts_pos =
          (struct argpos*)array_get(cur_rsp->args, start_idx_from_rsp - 1);

      rsp_mbuf->pos = ts_pos->end + 2;
      continue;
    }

    struct argpos* first_value_pos =
        (struct argpos*)array_get(cur_rsp->args, start_idx_from_rsp);

    if (num_values > 1) {
      // Here we're making space to write the total number of value tokens.
      // In reverse order, it's 2 for "\r\n", 'first_val_len_digits', 1 for "$",
      // 2 more for "\r\n", the number of token digits and finally 1 for "*".
      // If the value is an integer, we don't include the multibulk len and its
      // corressponding CRLF bytes.
      int go_back_by = 2 + num_value_digits + 1;
      if (*first_value_pos->start != ':') {
        int first_val_len_digits =
            count_digits(first_value_pos->end - first_value_pos->start);
        go_back_by += 2 + first_val_len_digits + 1;
      }
      rsp_mbuf->pos = first_value_pos->start - go_back_by;

      // Now we just need to write the "*" and the total number of tokens, the remaining
      // already exists as expected.
      *rsp_mbuf->pos = '*';
      int num_values_copy = num_values;
      int num_value_digits_copy = num_value_digits;
      while(num_value_digits_copy > 0) {
        int digit = num_values_copy % 10;
        *(rsp_mbuf->pos + num_value_digits_copy) = digit + '0';

        --num_value_digits_copy;
        num_values_copy /= 10;
      }
    } else {
      rsp_mbuf->pos = first_value_pos->start;

      // If it's an integer, we already have the right value.
      if (*rsp_mbuf->pos == ':') continue;

      // If the value is a string.
      rsp_mbuf->pos -= 2; // Go back 2 bytes to point behind the '\r\n' tokens.
      // Now go further back over the length of the token till we reach '$'
      while (*rsp_mbuf->pos != '$') {
        rsp_mbuf->pos -= 1;
      }
    }
  }
}

/*
 * Looks at responses in 'rspmgr' and determines if any of them are outdated.
 * If there exist outdated responses, it creates a repair msg which will repair
 * the peers that contained the outdated msg and returns it via the out param
 * 'new_msg_ptr'.
 *
 */
rstatus_t redis_make_repair_query(struct context *ctx, struct response_mgr *rspmgr,
    struct msg **new_msg_ptr) {

  if (is_read_repairs_enabled() == 0) return DN_OK;

  *new_msg_ptr = NULL;
  msg_type_t msg_type = rspmgr->msg->orig_type;
  if (msg_type == MSG_UNKNOWN) {
    msg_type = rspmgr->msg->type;
  }
  if (!proto_cmd_info[msg_type].is_repairable) return DN_OK;

  rstatus_t ret_status = DN_OK;

  struct msg* most_updated_rsp = NULL;
  bool repair_by_add = false;
  uint32_t num_values = 0;

  // If we enabled read repairs halfway, in flight commands will not be
  // repairable.
  if (rspmgr->msg->orig_msg == NULL) return DN_OK;

  // Redis commands either lookup keys or fields (secondary keys), so the number of
  // expected values would be based on either one of them.
  if (rspmgr->msg->orig_msg->msg_info.num_fields > 0) {
    num_values = rspmgr->msg->orig_msg->msg_info.num_fields;
  } else {
    num_values = rspmgr->msg->orig_msg->msg_info.num_keys;
  }

  most_updated_rsp = find_most_updated_rsp(rspmgr, &repair_by_add);
  // Avoid crafting a repair message if there's nothing to repair.
  if (most_updated_rsp == NULL) {
    ret_status = DN_OK;
    goto done;
  }

  struct write_with_ts repair_msg_info;
  THROW_STATUS(obtain_info_from_latest_rsp(rspmgr, most_updated_rsp,
      repair_by_add, &repair_msg_info));

  update_total_num_tokens(&repair_msg_info);

  ret_status = finalize_repair_msg(ctx, rspmgr->msg->owner, &repair_msg_info, new_msg_ptr);
  if (ret_status != DN_OK) {
    goto done;
  }

 done:
  adjust_rsp_buffers_for_client(rspmgr, num_values);
  return DN_OK;
}

/*
 * Converts 'orig_msg' to a Lua script that atomically does the original operation and
 * updates the metadata for that key(s), if that command has read repair support.
 *
 * Sets 'did_rewrite' to 'true' if the rewrite happened.
 *
 */
rstatus_t redis_rewrite_query_with_timestamp_md(struct msg *orig_msg, struct context *ctx,
    bool *did_rewrite, struct msg **new_msg_ptr) {

  ASSERT(orig_msg != NULL);
  ASSERT(orig_msg->is_request);
  ASSERT(did_rewrite != NULL);

  *did_rewrite = false;

  struct msg *new_msg = NULL;
  uint8_t *key = NULL;
  rstatus_t ret_status = DN_OK;

  // If we don't support read repairs for a command, return.
  if (proto_cmd_info[orig_msg->type].has_repair_support == false) return DN_OK;

  // If the parser couldn't parse the args correctly, return.
  if (orig_msg->rewrite_with_ts_possible == false) return DN_OK;

  rstatus_t status = post_parse_msg(orig_msg);
  if (status != DN_OK) goto error;

  update_total_num_tokens(&orig_msg->msg_info);

  ret_status = finalize_repair_msg(ctx, orig_msg->owner, &orig_msg->msg_info, new_msg_ptr);
  if (ret_status != DN_OK) goto error;
  *did_rewrite = true;
  return ret_status;

error:
  if (key != NULL) dn_free(key);
  // Return the newly allocated message back to the free message queue.
  if (new_msg != NULL) msg_put(new_msg);
  return ret_status;
}

// TODO: Do code cleanup
static rstatus_t create_cleanup_script(struct context *ctx, struct msg *orig_msg,
    struct conn *conn, struct msg **new_msg_ptr) {
  rstatus_t ret_status;

  struct write_with_ts msg_info;
  msg_info.keys = NULL;
  msg_info.fields = NULL;
  msg_info.num_keys = 1;
  msg_info.num_values = 0;
  msg_info.num_optionals = 0;

  msg_info.keys = array_create(msg_info.num_keys, sizeof(struct keypos));
  if (msg_info.keys == NULL) goto error;

  struct keypos *kpos = (struct keypos*)array_push(msg_info.keys);
  struct keypos *orig_kpos = (struct keypos*)array_get(orig_msg->keys, 0);
  kpos->start = orig_kpos->start;
  kpos->end = orig_kpos->end;
  kpos->tag_start = orig_kpos->tag_start;
  kpos->tag_end = orig_kpos->tag_end;

  msg_info.ts = orig_msg->timestamp;
  msg_info.add_set = ADD_SET_STR;
  msg_info.rem_set = REM_SET_STR;

  msg_type_t orig_msg_type = orig_msg->type;
  switch (orig_msg_type) {
    case MSG_REQ_REDIS_DEL:
      msg_info.rewrite_script = CLEANUP_DEL_SCRIPT;
      msg_info.num_fields = 0;
      break;
    case MSG_REQ_REDIS_ZREM:
    case MSG_REQ_REDIS_HDEL:
    case MSG_REQ_REDIS_SREM:
      msg_info.rewrite_script = CLEANUP_HDEL_SCRIPT;
      msg_info.num_fields = 1;
      msg_info.fields = array_create(msg_info.num_fields, sizeof(struct argpos));
      if (msg_info.fields == NULL) goto error;
      struct argpos *field_pos = (struct argpos*)array_push(msg_info.fields);
      struct argpos *orig_field_pos = (struct argpos*)array_get(orig_msg->args, 0);
      field_pos->start = orig_field_pos->start;
      field_pos->end = orig_field_pos->end;
      break;
    default:
      return DN_NOOPS;
      break;
  }
  // TODO: Consider adding a special type for cleanup scripts
  msg_info.cmd_type = MSG_UNKNOWN;

  update_total_num_tokens(&msg_info);

  ret_status = finalize_repair_msg(ctx, conn, &msg_info, new_msg_ptr);

  return ret_status;

 error:
  if (msg_info.keys != NULL) {
    array_destroy(msg_info.keys);
  }
  if (msg_info.fields != NULL) {
    array_destroy(msg_info.fields);
  }
  return DN_ERROR;
}

rstatus_t redis_clear_repair_md_for_key(struct context *ctx, struct msg *req,
    struct msg **new_msg_ptr) {
  // If we lost a track of the original message type, we cannot proceed.
  if (req->orig_msg == NULL) return DN_NOOPS;
  msg_type_t orig_msg_type = req->orig_msg->type;

  // If the original request wasn't a delete, then we shouldn't clear the metadata.
  if (proto_cmd_info[orig_msg_type].is_delete == false) return DN_NOOPS;

  // If we haven't received responses from all the replicas yet, we shouldn't clear the MD.
  if (++req->rspmgr.good_responses < req->rspmgr.max_responses) return DN_NOOPS;

  rstatus_t create_status = create_cleanup_script(
      ctx, req->orig_msg, req->owner, new_msg_ptr);

  // If we were unsuccessful in creating the script, do nothing.
  if (create_status != DN_OK) return DN_NOOPS;

  // This is a best effort command, don't attempt to capture any responses to it.
  (*new_msg_ptr)->expect_datastore_reply = false;
  (*new_msg_ptr)->awaiting_rsps = 0;

  //req_forward(ctx, req->owner, cleanup_msg);
  return DN_OK;
}
