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

#define GET_SCRIPT "$4\r\nEVAL\r\n$569\r\n"\
  "local key = KEYS[1]\n"\
  "local add_set = KEYS[2]\n"\
  "local rem_set = KEYS[3]\n"\
  "local orig_cmd = ARGV[1]\n"\
  "local num_fields = ARGV[2]\n"\
  "local cur_ts = ARGV[3]\n\n"\
  "local value = redis.call(orig_cmd, key)\n\n"\
  "local last_seen_ts_in_add = redis.call('ZSCORE', add_set, key)\n"\
  "if (last_seen_ts_in_add and value) then\n"\
  "  return {'E', last_seen_ts_in_add, value}\n"\
  "elseif (last_seen_ts_in_add) then\n"\
  "  redis.call('ZREM', add_set, key)\n"\
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

#define HDEL_SCRIPT "$4\r\nEVAL\r\n$1122\r\n"\
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
  "  redis.call('ZREM', top_level_rem_set, key)\n"\
  "end\n\n"\
  "return ret\n\r\n"

#define HGET_SCRIPT "$4\r\nEVAL\r\n$1055\r\n"\
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
  "  ts = tl_removed_ts\n"\
  "else\n"\
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
  "if (status_field == 'E' and not value) then\n"\
  "  return {'X', 0, value}\n"\
  "end\n"\
  "return {status_field, ts, value}\n\r\n"

#define ZADD_SCRIPT "$4\r\nEVAL\r\n$2022\r\n"\
  "local key = KEYS[1]\n"\
  "local top_level_add_set = KEYS[2]\n"\
  "local top_level_rem_set = KEYS[3]\n"\
  "local add_set = top_level_add_set .. '_' .. key\n"\
  "local rem_set = top_level_rem_set .. '_' .. key\n"\
  "local orig_cmd = ARGV[1]\n"\
  "local num_opts = ARGV[2]\n"\
  "local num_fields = ARGV[3]\n"\
  "local cur_ts = ARGV[4]\n"\
  "local start_loop = 5 + num_opts\n"\
  "local end_loop = (num_fields * 2) + 4 + num_opts\n"\
  "local top_level_rem_set_ts = redis.call('ZSCORE', top_level_rem_set, key)\n"\
  "if (top_level_rem_set_ts) then\n"\
  "  if (tonumber(cur_ts) < tonumber(top_level_rem_set_ts)) then\n"\
  "    return 0\n"\
  "  end\n"\
  "  redis.call('ZREM', top_level_rem_set, key)\n"\
  "end\n"\
  "local top_level_add_set_ts = redis.call('ZSCORE', top_level_add_set, key)\n"\
  "if (top_level_add_set_ts) then\n"\
  "  if (tonumber(cur_ts) > tonumber(top_level_add_set_ts)) then\n"\
  "    redis.call('ZADD', top_level_add_set, cur_ts, key)\n"\
  "  end\n"\
  "else\n"\
  "  redis.call('ZADD', top_level_add_set, cur_ts, key)\n"\
  "end\n"\
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
  "  end\n"\
  "  if (skiploop == false) then\n"\
  "    if (num_opts == '0') then\n"\
  "      ret = redis.call(orig_cmd, key, value, field)\n"\
  "    elseif (num_opts == '1') then\n"\
  "      ret = redis.call(orig_cmd, key, ARGV[5], value, field)\n"\
  "    elseif (num_opts == '2') then\n"\
  "      ret = redis.call(orig_cmd, key, ARGV[5], ARGV[6], value, field)\n"\
  "    elseif (num_opts == '3') then\n"\
  "      ret = redis.call(orig_cmd, key, ARGV[5], ARGV[6], ARGV[7], value, field)\n"\
  "    else\n"\
  "      ret = false\n"\
  "    end\n"\
  "    if (type(ret) ~= 'boolean') then\n"\
  "      redis.call('ZADD', add_set, cur_ts, field)\n"\
  "    end\n"\
  "  end\n"\
  "end\n"\
  "return ret\n\r\n"

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


/**********************************************************************/
/*                   BEGIN METADATA CLEANUP SCRIPTS                   */
/**********************************************************************/

// Note: Some of the fields are not necessary but are still kept because the Dynomite
// code creates all scripts with a certain argument format.
// Eg: 'orig_cmd', 'num_fields', etc. are unnecessary for these scripts.
#define CLEANUP_DEL_SCRIPT "$4\r\nEVAL\r\n$415\r\n"\
"local key = KEYS[1]\n"\
"local top_level_add_set = KEYS[2]\n"\
"local top_level_rem_set = KEYS[3]\n"\
"local orig_cmd = ARGV[1]\n"\
"local num_fields = ARGV[2]\n"\
"local cur_ts = ARGV[3]\n\n"\
"local top_level_rem_set_ts = redis.call('ZSCORE', top_level_rem_set, key)\n"\
"if (top_level_rem_set_ts) then\n"\
"  if (tonumber(cur_ts) < tonumber(top_level_rem_set_ts)) then\n"\
"    return 0\n"\
"  end\n"\
"  return redis.call('ZREM', top_level_rem_set, key)\n"\
"end\n"\
"return 0\n\r\n"

#define CLEANUP_HDEL_SCRIPT "$4\r\nEVAL\r\n$664\r\n"\
"local key = KEYS[1]\n"\
"local top_level_add_set = KEYS[2]\n"\
"local top_level_rem_set = KEYS[3]\n"\
"local add_set = top_level_add_set .. '_' .. key\n"\
"local rem_set = top_level_rem_set .. '_' .. key\n"\
"local orig_cmd = ARGV[1]\n"\
"local num_fields = ARGV[2]\n"\
"local cur_ts = ARGV[3]\n"\
"local field = ARGV[4]\n\n"\
"local last_seen_ts_in_rem = redis.call('ZSCORE', rem_set, field)\n"\
"if (last_seen_ts_in_rem) then\n"\
"  if (tonumber(cur_ts) < tonumber(last_seen_ts_in_rem)) then\n"\
"    return 0\n"\
"  end\n"\
"  local ret = redis.call('ZREM', rem_set, field)\n"\
"  local remaining_elems = redis.call('ZCARD', rem_set)\n"\
"  if (remaining_elems == 0) then\n"\
"    redis.call('ZREM', top_level_rem_set, key)\n"\
"  end\n"\
"  return ret\n"\
"end\n\r\n"

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

extern struct cmd_info proto_cmd_info[];

#endif
