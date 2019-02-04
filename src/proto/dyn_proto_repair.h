/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed
 * storages. Copyright (C) 2019 Netflix, Inc.
 */

#ifndef _DN_PROTO_REPAIR_H_
#define _DN_PROTO_REPAIR_H_

#include <ctype.h>
#include <stdbool.h>
#include <stdio.h>

#include "../dyn_message.h"

/*
 * In order to repair mismatched replicas, we need to repair them based on some agreed
 * upon value. In most systems that usually is based on the timestamps. Since Redis does
 * not support timestamps, we need to add the logic that tracks timestamps as metadata.
 *
 * The way we achieve recording timestamps is by converting all writes to Lua scripts that
 * atomically do the operation itself and store the metadata for that key.
 *
 * The metadata is stored in what we call 'add sets' and 'remove sets' which are basically
 * sorted sets that store the key name along with a corresponding timestamp that denotes
 * when that key was last updated. All primary keys go to a 'top level add/rem set' which
 * is currently denoted by '._add-set' and '._rem-set'.
 *
 * Secondary keys (fields in hash maps, sets, etc.) are stored in add/rem sets of their
 * corresponding primary keys and are denoted by '._add-set_<key>' and '._rem-set_<key>'.
 *
 * The 'redis_cmd_info' static array of structures contains information that will be used
 * by  the "rewrite with metadata" and "repair" logic. This array is indexed by the command
 * index that is set in dyn_message.h.
 *
 * All queries that will be rewritten to include metadata are denoted by
 * 'redis_cmd_info[CMD_IDX].has_repair_support' where 'CMD_IDX' is the index of the
 * corresponding query. All read queries that return the requested values along with the
 * metadata are denoted by 'redis_cmd_info[CMD_IDX].is_repairable'.
 * If queries do not have "rewrite with MD" or "repair" support, we fallback to the old
 * behavior which is based on quorum checksums.
 *
 * Every repairable read ('is_repairable' == true) returns data in the Redis wire protocol
 * in the following format:
 * <status> <ts> <value>
 *
 * , where status can be one of 'X', 'R' or 'E'. They stand for:
 *    X = value not found
 *    R = value was deleted (i.e. it was found in the rem-set)
 *    E = value exists (it was found in the add-set)
 *
 * This format was chosen because it's the most optimized in terms of returning values to
 * the client, i.e. we can just move the start pointer of the buffer just before '<value>'
 * by skipping '<status>' and '<ts>' thereby avoiding the need to copy the value to yet
 * another buffer.
 *
 * Currently the only read commands that have repair support return only one value at a
 * time. In the future when queries that can return multiple values will be returned,
 * the format will be naturally extended to be:
 * <status_1> <ts_1> <status_2> <ts_2> .. <status_n> <ts_n> <val_1> <val_2> .. <val_n>
 *
 * This will allow us to skip all the metadata with a simple pointer move just as we do
 * now.
 *
 * On repair supported reads, if we find that the timestamps between responses from the
 * responding replicas are not the same, we pick the replica that has the largest TS, take
 * its value and send a new write command to the replicas that are out of date with the
 * most updated value.
 *
 * Since we need to know the exact keys, fields and values while writing with metadata or
 * while repairing, we do a "post-parsing" step that records these in order to efficiently
 * populate the arguments for the Lua script that we will send to Redis on behalf of the
 * read/write. (see post_parse_msg())
 * This may change if we modify the parser itself to record these tokens efficiently.
 * TODO: Support parsing of optional fields.
 *
 * The Lua scripts that we use as templates for supported commands are listed below as
 * compile time constants.
 *
 * This is still in the beta stage and has some limitations:
 * - Large values may cause overflows ( > 512 bytes)
 * - Limited command support due to parser limitations:
 *     - Sorted sets and lists need optional command parsing support.
 * - Repairing on the read path vs. the background has perf implications. In the future,
 *   all the heavy lifting will happen in the background in order to have a 0 perf
 *   overhead on the read and write paths.
 *
 */
#define SET_SCRIPT "$4\r\nEVAL\r\n$640\r\n"\
  "local key = KEYS[1]\n"\
  "local add_set = KEYS[2]\n"\
  "local rem_set = KEYS[3]\n"\
  "local orig_cmd = ARGV[1]\n"\
  "local num_fields = ARGV[2]\n"\
  "local cur_ts = ARGV[3]\n"\
  "local value = ARGV[4]\n\n"\
  "local last_seen_ts_in_add = redis.call('ZSCORE', add_set, key)\n"\
  "local last_seen_ts_in_rem = redis.call('ZSCORE', rem_set, key)\n\n"\
  "if (last_seen_ts_in_rem) then\n"\
  "  if (tonumber(cur_ts) < tonumber(last_seen_ts_in_rem)) then\n"\
  "    return -1\n"\
  "  end\n"\
  "  redis.call('ZREM', rem_set, key)\n"\
  "elseif (last_seen_ts_in_add) then\n"\
  "  if (tonumber(cur_ts) < tonumber(last_seen_ts_in_add)) then\n"\
  "    return -1\n"\
  "  end\n"\
  "end\n\n"\
  "redis.call('ZADD', add_set, cur_ts, key)\n"\
  "return redis.call(orig_cmd, key, value)\n\r\n"

#define GET_SCRIPT "$4\r\nEVAL\r\n$490\r\n"\
  "local key = KEYS[1]\n"\
  "local add_set = KEYS[2]\n"\
  "local rem_set = KEYS[3]\n"\
  "local orig_cmd = ARGV[1]\n"\
  "local num_fields = ARGV[2]\n"\
  "local cur_ts = ARGV[3]\n\n"\
  "local value = redis.call(orig_cmd, key)\n\n"\
  "local last_seen_ts_in_add = redis.call('ZSCORE', add_set, key)\n"\
  "if (last_seen_ts_in_add) then\n"\
  "  return {'E', last_seen_ts_in_add, value}\n"\
  "end\n\n"\
  "local last_seen_ts_in_rem = redis.call('ZSCORE', rem_set, key)\n"\
  "if (last_seen_ts_in_rem) then\n"\
  "  return {'R', last_seen_ts_in_rem, value}\n"\
  "end\n\n"\
  "return {'X', 0, value}\n\r\n"

#define DEL_SCRIPT "$4\r\nEVAL\r\n$793\r\n"\
  "local key = KEYS[1]\n"\
  "local add_set = KEYS[2]\n"\
  "local rem_set = KEYS[3]\n"\
  "local orig_cmd = ARGV[1]\n"\
  "local num_fields = ARGV[2]\n"\
  "local cur_ts = ARGV[3]\n\n"\
  "local last_seen_ts_in_add = redis.call('ZSCORE', add_set, key)\n"\
  "local last_seen_ts_in_rem = redis.call('ZSCORE', rem_set, key)\n"\
  "if (last_seen_ts_in_rem) then\n"\
  "  if (tonumber(cur_ts) < tonumber(last_seen_ts_in_rem)) then\n"\
  "    return 0\n"\
  "  end\n"\
  "  redis.call('ZREM', rem_set, key)\n"\
  "elseif (last_seen_ts_in_add) then\n"\
  "  return 0\n"\
  "end\n\n"\
  "local exists = redis.call('EXISTS', key)\n"\
  "if (exists) then\n"\
  "  local composite_add_set = add_set .. '_' .. key\n"\
  "  local composite_rem_set = rem_set .. '_' .. key\n"\
  "  redis.call('DEL', composite_add_set)\n"\
  "  redis.call('DEL', composite_rem_set)\n\n"\
  "  redis.call('ZADD', add_set, cur_ts, key)\n"\
  "  return redis.call(orig_cmd, key)\n"\
  "end\n"\
  "return 0\n\r\n"

#define HSET_SCRIPT "$4\r\nEVAL\r\n$1613\r\n"\
  "local key = KEYS[1]\n"\
  "local top_level_add_set = KEYS[2]\n"\
  "local top_level_rem_set = KEYS[3]\n"\
  "local add_set = top_level_add_set .. '_' .. key\n"\
  "local rem_set = top_level_rem_set .. '_' .. key\n"\
  "local orig_cmd = ARGV[1]\n"\
  "local num_fields = ARGV[2]\n"\
  "local cur_ts = ARGV[3]\n\n"\
  "local start_loop = 4\n"\
  "local end_loop = (num_fields * 2) + 3\n\n"\
  "local top_level_rem_set_ts = redis.call('ZSCORE', top_level_rem_set, key)\n"\
  "if (top_level_rem_set_ts) then\n"\
  "  if (tonumber(cur_ts) < tonumber(top_level_rem_set_ts)) then\n"\
  "    return 0\n"\
  "  end\n"\
  "  redis.call('ZREM', top_level_rem_set, key)\n"\
  "end\n\n"\
  "local top_level_add_set_ts = redis.call('ZSCORE', top_level_add_set, key)\n"\
  "if (top_level_add_set_ts) then\n"\
  "  if (tonumber(cur_ts) > tonumber(top_level_add_set_ts)) then\n"\
  "    redis.call('ZADD', top_level_add_set, cur_ts, key)\n"\
  "  end\n"\
  "else\n"\
  "  redis.call('ZADD', top_level_add_set, cur_ts, key)\n"\
  "end\n\n"\
  "local skiploop\n"\
  "local ret\n"\
  "for i=start_loop,end_loop,2\n"\
  "do\n"\
  "  skiploop = false\n"\
  "  local field = ARGV[i]\n"\
  "  local value = ARGV[i+1]\n"\
  "  local last_seen_ts_in_add = redis.call('ZSCORE', add_set, field)\n"\
  "  local last_seen_ts_in_rem = redis.call('ZSCORE', rem_set, field)\n"\
  "  if (last_seen_ts_in_rem) then\n"\
  "    if (tonumber(cur_ts) < tonumber(last_seen_ts_in_rem)) then\n"\
  "      skiploop = true\n"\
  "    end\n"\
  "    redis.call('ZREM', rem_set, field)\n"\
  "  elseif (last_seen_ts_in_add) then\n"\
  "    if (tonumber(cur_ts) < tonumber(last_seen_ts_in_add)) then\n"\
  "      skiploop = true\n"\
  "    end\n"\
  "  end\n\n"\
  "  if (skiploop == false) then\n"\
  "    redis.call('ZADD', add_set, cur_ts, field)\n"\
  "    ret = redis.call(orig_cmd, key, field, value)\n"\
  "  end\n"\
  "end\n\n"\
  "if tonumber(num_fields) > 1 then\n"\
  "  return \"OK\"\n"\
  "else\n"\
  "  return ret\n"\
  "end\n\r\n"

#define HDEL_SCRIPT "$4\r\nEVAL\r\n$1175\r\n"\
  "local key = KEYS[1]\n"\
  "local top_level_add_set = KEYS[2]\n"\
  "local top_level_rem_set = KEYS[3]\n"\
  "local add_set = top_level_add_set .. '_' .. key\n"\
  "local rem_set = top_level_rem_set .. '_' .. key\n"\
  "local orig_cmd = ARGV[1]\n"\
  "local num_fields = ARGV[2]\n"\
  "local cur_ts = ARGV[3]\n\n"\
  "local start_loop = 4\n"\
  "local end_loop = num_fields + 3\n\n"\
  "local skiploop\n"\
  "local ret = 0\n"\
  "for i=start_loop,end_loop,1\n"\
  "do\n"\
  "  skiploop = false\n"\
  "  local field = ARGV[i]\n"\
  "  local last_seen_ts_in_add = redis.call('ZSCORE', add_set, field)\n"\
  "  local last_seen_ts_in_rem = redis.call('ZSCORE', rem_set, field)\n"\
  "  if (last_seen_ts_in_rem) then\n"\
  "    if (tonumber(cur_ts) < tonumber(last_seen_ts_in_rem)) then\n"\
  "      skiploop = true\n"\
  "    else\n"\
  "      redis.call('ZREM', rem_set, field)\n"\
  "    end\n"\
  "  elseif (last_seen_ts_in_add) then\n"\
  "    if (tonumber(cur_ts) < tonumber(last_seen_ts_in_add)) then\n"\
  "      skiploop = true\n"\
  "    end\n"\
  "  end\n\n"\
  "  if (skiploop == false) then\n"\
  "    redis.call('ZADD', add_set, cur_ts, field)\n"\
  "    ret = ret + redis.call(orig_cmd, key, field)\n"\
  "  end\n"\
  "end\n\n"\
  "local card = redis.call('ZCARD', rem_set)\n"\
  "if (card == 0) then\n"\
  "  redis.call('ZADD', top_level_add_set, cur_ts, key)\n"\
  "  redis.call('ZREM', top_level_rem_set, key)\n"\
  "end\n\n"\
  "return ret\n\r\n"

#define HGET_SCRIPT "$4\r\nEVAL\r\n$982\r\n"\
  "local key = KEYS[1]\n"\
  "local top_level_add_set = KEYS[2]\n"\
  "local top_level_rem_set = KEYS[3]\n"\
  "local add_set = top_level_add_set .. '_' .. key\n"\
  "local rem_set = top_level_rem_set .. '_' .. key\n"\
  "local orig_cmd = ARGV[1]\n"\
  "local num_fields = ARGV[2]\n"\
  "local cur_ts = ARGV[3]\n"\
  "local field = ARGV[4]\n\n"\
  "local status_field = 'E'\n"\
  "local ts = 0\n\n"\
  "local tl_removed_ts = redis.call('ZSCORE', top_level_rem_set, key)\n"\
  "if (tl_removed_ts) then\n"\
  "  status_field = 'R'\n"\
  "  ts = tl_removed_ts\nelse\n"\
  "  local removed_ts = redis.call('ZSCORE', rem_set, field)\n"\
  "  if (removed_ts) then\n"\
  "    ts = removed_ts\n"\
  "    status_field = 'R'\n"\
  "  end\n"\
  "end\n\n"\
  "if (status_field ~= 'R') then\n"\
  "  local tl_exists = redis.call('ZSCORE', top_level_add_set, key)\n"\
  "  if (tl_exists) then\n"\
  "    local exists_ts = redis.call('ZSCORE', add_set, field)\n"\
  "    if (not exists_ts) then\n"\
  "      status_field = 'X'\n"\
  "    else\n"\
  "      ts = exists_ts\n"\
  "    end\n"\
  "  else\n"\
  "    status_field = 'X'\n"\
  "  end\n"\
  "end\n\n"\
  "local value = redis.call(orig_cmd, key, field)\n"\
  "return {status_field, ts, value}\n\r\n"

#define SADD_SCRIPT "$4\r\nEVAL\r\n$1526\r\n"\
  "local key = KEYS[1]\n"\
  "local top_level_add_set = KEYS[2]\n"\
  "local top_level_rem_set = KEYS[3]\n"\
  "local add_set = top_level_add_set .. '_' .. key\n"\
  "local rem_set = top_level_rem_set .. '_' .. key\n"\
  "local orig_cmd = ARGV[1]\n"\
  "local num_fields = ARGV[2]\n"\
  "local cur_ts = ARGV[3]\n\n"\
  "local start_loop = 4\n"\
  "local end_loop = num_fields + 3\n\n"\
  "local top_level_rem_set_ts = redis.call('ZSCORE', top_level_rem_set, key)\n"\
  "if (top_level_rem_set_ts) then\n"\
  "  if (tonumber(cur_ts) < tonumber(top_level_rem_set_ts)) then\n"\
  "    return 0\n"\
  "  end\n"\
  "  redis.call('ZREM', top_level_rem_set, key)\n"\
  "end\n\n"\
  "local top_level_add_set_ts = redis.call('ZSCORE', top_level_add_set, key)\n"\
  "if (top_level_add_set_ts) then\n"\
  "  if (tonumber(cur_ts) > tonumber(top_level_add_set_ts)) then\n"\
  "    redis.call('ZADD', top_level_add_set, cur_ts, key)\n"\
  "  end\n"\
  "else\n"\
  "  redis.call('ZADD', top_level_add_set, cur_ts, key)\n"\
  "end\n\n"\
  "local skiploop\n"\
  "local ret = 0\n"\
  "for i=start_loop,end_loop,1\n"\
  "do\n"\
  "  skiploop = false\n"\
  "  local field = ARGV[i]\n"\
  "  local last_seen_ts_in_add = redis.call('ZSCORE', add_set, field)\n"\
  "  local last_seen_ts_in_rem = redis.call('ZSCORE', rem_set, field)\n"\
  "  if (last_seen_ts_in_rem) then\n"\
  "    if (tonumber(cur_ts) < tonumber(last_seen_ts_in_rem)) then\n"\
  "      skiploop = true\n"\
  "    end\n"\
  "    redis.call('ZREM', rem_set, field)\n"\
  "  elseif (last_seen_ts_in_add) then\n"\
  "    if (tonumber(cur_ts) < tonumber(last_seen_ts_in_add)) then\n"\
  "      skiploop = true\n"\
  "    end\n"\
  "  end\n\n"\
  "  if (skiploop == false) then\n"\
  "    redis.call('ZADD', add_set, cur_ts, field)\n"\
  "    ret = ret + redis.call(orig_cmd, key, field)\n"\
  "  end\n"\
  "end\n\n"\
  "return ret\n\r\n"

#define MAX_ARG_FMT_STR_LEN 512

#define ADD_SET_STR "._add-set"
#define REM_SET_STR "._rem-set"

struct cmd_info {
  char cmd_str[20];
  uint32_t num_args;
  uint32_t min_num_keys;
  uint32_t min_num_fields;
  uint32_t min_num_values;
  uint32_t num_optionals;
  bool has_repair_support;
  bool is_delete;
  bool is_repairable;
  int first_key_pos;
  int variadic_key_jump;
  int first_field_pos;
  int variadic_field_jump;
  int first_value_pos;
  int variadic_value_jump;
  const char* rewrite_script;
  msg_type_t repair_by_add;
  msg_type_t repair_by_rem;
};

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
  {"ZADD", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
  {"ZCARD", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
  {"ZCOUNT", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
  {"ZINCRBY", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
  {"ZINTERSTORE", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
  {"ZLEXCOUNT", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
  {"ZRANGE", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
  {"ZRANGEBYLEX", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
  {"ZRANGEBYSCORE", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
  {"ZRANK", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
  {"ZREM", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
  {"ZREMRANGEBYRANK", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
  {"ZREMRANGEBYLEX", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
  {"ZREMRANGEBYSCORE", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
  {"ZREVRANGE", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
  {"ZREVRANGEBYLEX", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
  {"ZREVRANGEBYSCORE", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
  {"ZREVRANK", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
  {"ZSCORE", 0, 0, 0, 0, 0, false, false, false, -1, -1, -1, -1, -1, -1, NULL, 0, 0},
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

#endif
