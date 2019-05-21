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

/*
 * Parses the original 'struct msg' and fills in 'orig_msg->msg_info' with all
 * the keys, fields, values and optional fields.
 *
 * TODO: Consider getting rid of this function if we move to a per-command parser model.
 *
 */
rstatus_t post_parse_msg(struct msg *orig_msg) {

  struct write_with_ts *out_struct = &orig_msg->msg_info;
  ASSERT(out_struct != NULL);

  out_struct->cmd_type = orig_msg->type;
  out_struct->ts = orig_msg->timestamp;
  struct cmd_info *orig_cmd_info = &proto_cmd_info[orig_msg->type];
  out_struct->num_fields = orig_cmd_info->min_num_fields;
  out_struct->num_values = orig_cmd_info->min_num_values;
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

  out_struct->num_fields = total_tokens_of_type(
      orig_cmd_info->variadic_field_jump,
      orig_cmd_info->first_field_pos - 1 /* start_pos */,
      orig_cmd_info->min_num_fields,
      orig_msg->args->nelem);

  if (parse_tokens_of_type(out_struct->num_fields, 0,
          orig_cmd_info->variadic_field_jump, orig_cmd_info->first_field_pos,
          orig_msg->args, false, &out_struct->fields) != DN_OK) {
    goto error;
  }

  out_struct->num_values = total_tokens_of_type(
      orig_cmd_info->variadic_value_jump,
      orig_cmd_info->first_value_pos - 1 /* start_pos */,
      orig_cmd_info->min_num_values,
      orig_msg->args->nelem);

  if (parse_tokens_of_type(out_struct->num_values, 0,
          orig_cmd_info->variadic_value_jump, orig_cmd_info->first_value_pos,
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
  int i = 0;

  // Copy all the relevant information from the 'most_updated_rsp' to craft a
  // 'struct write_with_ts' to be used while creating the repair msg.
  repair_msg_info->cmd_type = repair_msg_type;
  repair_msg_info->rewrite_script = proto_cmd_info[repair_msg_type].rewrite_script;
  repair_msg_info->ts = most_updated_rsp->timestamp;

  repair_msg_info->num_keys = orig_msg_info->num_keys;
  repair_msg_info->num_fields = orig_msg_info->num_fields;
  repair_msg_info->num_values = orig_msg_info->num_values;

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

/*
 * Appends a string argument based on the Redis wire protocol to the string pointed to by
 * 'out_str_ptr' and moves the pointer forward by the number of bytes appended.
 *
 * Returns the total number of bytes appended.
 */
static size_t append_redis_prtcl_varchar_arg_to_str(char *arg, int len,
    char** out_str_ptr) {
  int n = sprintf(*out_str_ptr, "$%d\r\n%.*s\r\n", len, len, arg);
  *out_str_ptr += n;

  return n;
}

static size_t append_redis_prtcl_int_arg_to_str(int arg, int len,
    char** out_str_ptr) {
  int n = sprintf(*out_str_ptr, "$%d\r\n%d\r\n", len, arg);
  *out_str_ptr += n;

  return n;
}


static size_t append_redis_prtcl_llu_arg_to_str(unsigned long long arg, int len,
    char** out_str_ptr) {
  int n = sprintf(*out_str_ptr, "$%d\r\n%llu\r\n", len, arg);
  *out_str_ptr += n;

  return n;
}

/*
 * Using the information found in 'write_with_ts', this function populates 'out_fmt_str'
 * with an argument string based on the Redis wire protocol that will be passed to a
 * Lua script.
 *
 * It also updates 'src' with the total number of tokens.
 *
 * The format is:
 * <key1>..(<keyN>) <+set> <-set> <orig_cmd> <num_flds> <ts> (<fld1>) (<val1>) (<fldN>) ..
 *
 * Tokens shown above with parantheses are optional.
 *
 * Returns the total size of the final stirng.
 *
 */
static size_t create_redis_prtcl_script_args(
    struct write_with_ts *src, char *out_fmt_str) {
  ASSERT(out_fmt_str != NULL);

  int n, i;
  int total_len = 0;
  char *str_ptr = out_fmt_str;

  int num_keys = src->num_keys;
  int num_fields = src->num_fields;
  int num_values = src->num_values;

  // Add 2 by default, one for the 'EVAL' command and one for the script itself.
  src->total_num_tokens = 2;

  // Adding 2 to 'num_keys' for the add and remove sets.
  total_len += append_redis_prtcl_int_arg_to_str(
      num_keys + 2, count_digits(num_keys + 2), &str_ptr);
  ++src->total_num_tokens;

  // Add all the keys touched in the query.
  for (i = 0; i < src->num_keys; ++i) {
    struct keypos *elem = array_get(src->keys, i);
    uint32_t elem_len = keypos_elem_len(elem);
    total_len += append_redis_prtcl_varchar_arg_to_str(
        (char*)elem->tag_start, elem_len, &str_ptr);
    ++src->total_num_tokens;
  }
  // Add the add-set and rem-set keys.
  int add_set_len = strlen(src->add_set);
  int rem_set_len = strlen(src->rem_set);
  total_len += append_redis_prtcl_varchar_arg_to_str(src->add_set, add_set_len, &str_ptr);
  total_len += append_redis_prtcl_varchar_arg_to_str(src->rem_set, rem_set_len, &str_ptr);
  src->total_num_tokens += 2;

  // Add the command string. (Eg: SET, HSET, etc.)
  char *orig_cmd_str = proto_cmd_info[src->cmd_type].cmd_str;
  total_len += append_redis_prtcl_varchar_arg_to_str(
      orig_cmd_str, strlen(orig_cmd_str), &str_ptr);

  // Add the number of fields.
  total_len += append_redis_prtcl_int_arg_to_str(
      num_fields, count_digits(num_fields), &str_ptr);

  // Add the timestamp.
  int num_ts_digits = count_digits(src->ts);
  total_len += append_redis_prtcl_llu_arg_to_str(
      src->ts, count_digits(src->ts), &str_ptr);

  src->total_num_tokens += 3;

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
        total_len += append_redis_prtcl_varchar_arg_to_str(
            (char*)field_elem->start, field_len, &str_ptr);
        ++src->total_num_tokens;
      }

      if (num_values > 0) {
        struct argpos *value_elem = array_get(src->values, i);
        uint32_t value_len = argpos_elem_len(value_elem);
        total_len += append_redis_prtcl_varchar_arg_to_str(
            (char*)value_elem->start, value_len, &str_ptr);
        ++src->total_num_tokens;
      }
    }
  }

  // NULL terminate the string.
  strcpy(str_ptr, "\0");
  return total_len;
}

/*
 * Uses 'msg_info' and 'arg_str' to create a repair msg and parse it so that its ready
 * to be understood by Redis.
 *
 */
static rstatus_t finalize_repair_msg(struct context *ctx, struct conn *conn,
    struct write_with_ts *msg_info, char* arg_str,  struct msg **new_msg_ptr) {

  rstatus_t ret_status;
  struct msg *new_msg = NULL;
  new_msg = msg_get(conn, true, __FUNCTION__);
  if (new_msg == NULL) {
    ret_status = DN_ENOMEM;
    goto error;
  }

  // Prepend the total number of tokens as mandated by the Redis wire protocol, followed
  // by the script and finally the argument string to the script.
  ret_status = msg_prepend_format(new_msg, "*%d\r\n%s%s", 3 /* num_args */,
      msg_info->total_num_tokens, msg_info->rewrite_script, arg_str);
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

  if (ctx->read_repairs_enabled == 0) return DN_OK;

  *new_msg_ptr = NULL;
  msg_type_t msg_type = rspmgr->msg->orig_type;
  if (msg_type == MSG_UNKNOWN) {
    msg_type = rspmgr->msg->type;
  }
  if (!proto_cmd_info[msg_type].is_repairable) return DN_OK;

  struct msg *new_msg = NULL;
  rstatus_t ret_status = DN_OK;

  int i;
  uint64_t biggest_ts = 0;
  struct msg* most_updated_rsp = NULL;
  bool repair_by_add = false;
  bool at_least_one_repair = false;
  uint32_t num_values = 0;

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
  rstatus_t status = obtain_info_from_latest_rsp(rspmgr, most_updated_rsp,
      repair_by_add, &repair_msg_info);

  // TODO: Dynamically allocate 'arg_fmt_str'.
  char arg_fmt_str[MAX_ARG_FMT_STR_LEN];
  size_t arg_fmt_str_len = create_redis_prtcl_script_args(&repair_msg_info, (char*)&arg_fmt_str);

  ret_status = finalize_repair_msg(ctx, rspmgr->msg->owner, &repair_msg_info,
      (char*)&arg_fmt_str, new_msg_ptr);
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

  char arg_fmt_str[MAX_ARG_FMT_STR_LEN];
  size_t arg_fmt_str_len = create_redis_prtcl_script_args(
      &orig_msg->msg_info, (char*)&arg_fmt_str);

  ret_status = finalize_repair_msg(ctx, orig_msg->owner, &orig_msg->msg_info,
      (char*)&arg_fmt_str, new_msg_ptr);
  if (ret_status != DN_OK) goto error;
  *did_rewrite = true;
  return ret_status;

error:
  if (key != NULL) dn_free(key);
  // Return the newly allocated message back to the free message queue.
  if (new_msg != NULL) msg_put(new_msg);
  return ret_status;
}
