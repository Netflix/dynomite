/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed
 * storages. Copyright (C) 2014 Netflix, Inc.
 */

/*
 * twemproxy - A fast and lightweight proxy for memcached protocol.
 * Copyright (C) 2011 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <ctype.h>
#include <stdio.h>

#include "../dyn_core.h"
#include "../dyn_dnode_peer.h"
#include "../dyn_util.h"
#include "dyn_proto.h"
#include "dyn_proto_repair.h"

#define RSP_STRING(ACTION) ACTION(ok, "+OK\r\n")

/*ACTION( pong,             "+PONG\r\n"                   ) \ */

#define DEFINE_ACTION(_var, _str) \
  static struct string rsp_##_var = string(_str);
RSP_STRING(DEFINE_ACTION)
#undef DEFINE_ACTION

/*
 * Return true, if the redis command take no key, otherwise
 * return false
 */
static bool redis_argz(struct msg *r) {
  switch (r->type) {
    case MSG_REQ_REDIS_PING:
    case MSG_REQ_REDIS_QUIT:
    case MSG_REQ_REDIS_SCRIPT_FLUSH:
    case MSG_REQ_REDIS_SCRIPT_KILL:
      return true;

    default:
      break;
  }

  return false;
}

/*
 * Return true, if the redis command accepts no arguments, otherwise
 * return false
 */
static bool redis_arg0(struct msg *r) {
  switch (r->type) {
    case MSG_REQ_REDIS_PERSIST:
    case MSG_REQ_REDIS_PTTL:
    case MSG_REQ_REDIS_TTL:
    case MSG_REQ_REDIS_TYPE:
    case MSG_REQ_REDIS_DUMP:

    case MSG_REQ_REDIS_DECR:
    case MSG_REQ_REDIS_GET:
    case MSG_REQ_REDIS_INCR:
    case MSG_REQ_REDIS_STRLEN:

    case MSG_REQ_REDIS_HGETALL:
    case MSG_REQ_REDIS_HKEYS:
    case MSG_REQ_REDIS_HLEN:
    case MSG_REQ_REDIS_HVALS:

    case MSG_REQ_REDIS_LLEN:
    case MSG_REQ_REDIS_LPOP:
    case MSG_REQ_REDIS_RPOP:

    case MSG_REQ_REDIS_SCARD:
    case MSG_REQ_REDIS_SMEMBERS:
    case MSG_REQ_REDIS_SRANDMEMBER:

    case MSG_REQ_REDIS_ZCARD:

    case MSG_REQ_REDIS_KEYS:
    case MSG_REQ_REDIS_PFCOUNT:
      return true;

    default:
      break;
  }

  return false;
}

/*
 * Return true, if the redis command accepts exactly 1 argument, otherwise
 * return false
 */
static bool redis_arg1(struct msg *r) {
  switch (r->type) {
    case MSG_REQ_REDIS_EXPIRE:
    case MSG_REQ_REDIS_EXPIREAT:
    case MSG_REQ_REDIS_PEXPIRE:
    case MSG_REQ_REDIS_PEXPIREAT:

    case MSG_REQ_REDIS_APPEND:
    case MSG_REQ_REDIS_DECRBY:
    case MSG_REQ_REDIS_GETBIT:
    case MSG_REQ_REDIS_GETSET:
    case MSG_REQ_REDIS_INCRBY:
    case MSG_REQ_REDIS_INCRBYFLOAT:
    case MSG_REQ_REDIS_SETNX:

    case MSG_REQ_REDIS_HEXISTS:
    case MSG_REQ_REDIS_HGET:

    case MSG_REQ_REDIS_LINDEX:
    case MSG_REQ_REDIS_LPUSHX:
    case MSG_REQ_REDIS_RPOPLPUSH:
    case MSG_REQ_REDIS_RPUSHX:

    case MSG_REQ_REDIS_SISMEMBER:

    case MSG_REQ_REDIS_ZRANK:
    case MSG_REQ_REDIS_ZREVRANK:
    case MSG_REQ_REDIS_ZSCORE:
    case MSG_REQ_REDIS_SLAVEOF:
    case MSG_REQ_REDIS_CONFIG:
    case MSG_REQ_REDIS_SCRIPT_LOAD:
    case MSG_REQ_REDIS_SCRIPT_EXISTS:

      return true;

    default:
      break;
  }

  return false;
}

static bool redis_arg_upto1(struct msg *r) {
  switch (r->type) {
    case MSG_REQ_REDIS_INFO:
      return true;
    default:
      break;
  }
  return false;
}
/*
 * Return true, if the redis command accepts exactly 2 arguments, otherwise
 * return false
 */
static bool redis_arg2(struct msg *r) {
  switch (r->type) {
    case MSG_REQ_REDIS_GETRANGE:
    case MSG_REQ_REDIS_PSETEX:
    case MSG_REQ_REDIS_SETBIT:
    case MSG_REQ_REDIS_SETEX:
    case MSG_REQ_REDIS_SETRANGE:

    case MSG_REQ_REDIS_HINCRBY:
    case MSG_REQ_REDIS_HINCRBYFLOAT:
    case MSG_REQ_REDIS_HSET:
    case MSG_REQ_REDIS_HSETNX:

    case MSG_REQ_REDIS_LRANGE:
    case MSG_REQ_REDIS_LREM:
    case MSG_REQ_REDIS_LSET:
    case MSG_REQ_REDIS_LTRIM:

    case MSG_REQ_REDIS_SMOVE:

    case MSG_REQ_REDIS_ZCOUNT:
    case MSG_REQ_REDIS_ZINCRBY:
    case MSG_REQ_REDIS_ZLEXCOUNT:
    case MSG_REQ_REDIS_ZREMRANGEBYLEX:
    case MSG_REQ_REDIS_ZREMRANGEBYRANK:
    case MSG_REQ_REDIS_ZREMRANGEBYSCORE:

    case MSG_REQ_REDIS_RESTORE:

      return true;

    default:
      break;
  }

  return false;
}

/*
 * Return true, if the redis command accepts exactly 3 arguments, otherwise
 * return false
 */
static bool redis_arg3(struct msg *r) {
  switch (r->type) {
    case MSG_REQ_REDIS_LINSERT:
      return true;

    default:
      break;
  }

  return false;
}

/*
 * Return true, if the redis command accepts 0 or more arguments, otherwise
 * return false
 */
static bool redis_argn(struct msg *r) {
  switch (r->type) {
    case MSG_REQ_REDIS_SORT:

    case MSG_REQ_REDIS_BITCOUNT:

    case MSG_REQ_REDIS_SET:
    case MSG_REQ_REDIS_SCAN:
    case MSG_REQ_REDIS_HDEL:
    case MSG_REQ_REDIS_HMGET:
    case MSG_REQ_REDIS_HMSET:
    case MSG_REQ_REDIS_HSCAN:

    case MSG_REQ_REDIS_LPUSH:
    case MSG_REQ_REDIS_RPUSH:

    case MSG_REQ_REDIS_SADD:
    case MSG_REQ_REDIS_SDIFF:
    case MSG_REQ_REDIS_SDIFFSTORE:
    case MSG_REQ_REDIS_SINTER:
    case MSG_REQ_REDIS_SINTERSTORE:
    case MSG_REQ_REDIS_SREM:
    case MSG_REQ_REDIS_SUNION:
    case MSG_REQ_REDIS_SUNIONSTORE:
    case MSG_REQ_REDIS_SSCAN:
    case MSG_REQ_REDIS_SPOP:

    case MSG_REQ_REDIS_ZADD:
    case MSG_REQ_REDIS_ZINTERSTORE:
    case MSG_REQ_REDIS_ZRANGE:
    case MSG_REQ_REDIS_ZRANGEBYSCORE:
    case MSG_REQ_REDIS_ZREM:
    case MSG_REQ_REDIS_ZREVRANGE:
    case MSG_REQ_REDIS_ZRANGEBYLEX:
    case MSG_REQ_REDIS_ZREVRANGEBYLEX:
    case MSG_REQ_REDIS_ZREVRANGEBYSCORE:
    case MSG_REQ_REDIS_ZUNIONSTORE:
    case MSG_REQ_REDIS_ZSCAN:
    case MSG_REQ_REDIS_PFADD:
    case MSG_REQ_REDIS_GEOADD:
    case MSG_REQ_REDIS_GEORADIUS:
    case MSG_REQ_REDIS_GEODIST:
    case MSG_REQ_REDIS_GEOHASH:
    case MSG_REQ_REDIS_GEOPOS:
    case MSG_REQ_REDIS_GEORADIUSBYMEMBER:

    case MSG_REQ_REDIS_JSONSET:
    case MSG_REQ_REDIS_JSONGET:
    case MSG_REQ_REDIS_JSONDEL:
    case MSG_REQ_REDIS_JSONTYPE:
    case MSG_REQ_REDIS_JSONMGET:
    case MSG_REQ_REDIS_JSONARRAPPEND:
    case MSG_REQ_REDIS_JSONARRINSERT:
    case MSG_REQ_REDIS_JSONARRLEN:
    case MSG_REQ_REDIS_JSONOBJKEYS:
    case MSG_REQ_REDIS_JSONOBJLEN:
      return true;

    default:
      break;
  }

  return false;
}

/*
 * Return true, if the redis command is a vector command accepting one or
 * more keys, otherwise return false
 */
static bool redis_argx(struct msg *r) {
  switch (r->type) {
    case MSG_REQ_REDIS_MGET:
    case MSG_REQ_REDIS_DEL:
    case MSG_REQ_REDIS_EXISTS:
      return true;

    default:
      break;
  }

  return false;
}

/*
 * Return true, if the redis command is a vector command accepting one or
 * more key-value pairs, otherwise return false
 */
static bool redis_argkvx(struct msg *r) {
  switch (r->type) {
    case MSG_REQ_REDIS_MSET:
      return true;

    default:
      break;
  }

  return false;
}

/*
 * Return true, if the redis command is either EVAL or EVALSHA. These commands
 * have a special format with exactly 2 arguments, followed by one or more keys,
 * followed by zero or more arguments (the documentation online seems to suggest
 * that at least one argument is required, but that shouldn't be the case).
 */
static bool redis_argeval(struct msg *r) {
  switch (r->type) {
    case MSG_REQ_REDIS_EVAL:
    case MSG_REQ_REDIS_EVALSHA:
      return true;

    default:
      break;
  }

  return false;
}

/*
 * Return true, if the redis response is an error response i.e. a simple
 * string whose first character is '-', otherwise return false.
 */
static bool redis_error(struct msg *r) {
  switch (r->type) {
    case MSG_RSP_REDIS_ERROR:
    case MSG_RSP_REDIS_ERROR_ERR:
    case MSG_RSP_REDIS_ERROR_OOM:
    case MSG_RSP_REDIS_ERROR_BUSY:
    case MSG_RSP_REDIS_ERROR_NOAUTH:
    case MSG_RSP_REDIS_ERROR_LOADING:
    case MSG_RSP_REDIS_ERROR_BUSYKEY:
    case MSG_RSP_REDIS_ERROR_MISCONF:
    case MSG_RSP_REDIS_ERROR_NOSCRIPT:
    case MSG_RSP_REDIS_ERROR_READONLY:
    case MSG_RSP_REDIS_ERROR_WRONGTYPE:
    case MSG_RSP_REDIS_ERROR_EXECABORT:
    case MSG_RSP_REDIS_ERROR_MASTERDOWN:
    case MSG_RSP_REDIS_ERROR_NOREPLICAS:
      return true;

    default:
      break;
  }
  return false;
}

rstatus_t record_arg(uint8_t* start_pos, uint8_t* end_pos, struct array *target_arr) {
  ASSERT(target_arr != NULL && start_pos != NULL && end_pos != NULL);

  struct argpos *arg_pos;
  arg_pos = array_push(target_arr);
  if (arg_pos == NULL) {
    return DN_ENOMEM;
  }
  arg_pos->start = start_pos;
  arg_pos->end = end_pos;

  return DN_OK;
}

/*
 * Detects the query and does a rewrite if applicable.
 *
 * Currently the following queries are rewritten:
 * 1) SMEMBERS <set> -> SORT <myset> ALPHA (only when DC_SAFE_QUORUM=true)
 *    We rewrite this query this way since when DC_SAFE_QUORUM is enabled,
 *    we run this query on multiple nodes and take checksums and compare that
 *    they're the same. Since SMEMBERS offers no ordering guarantee, even though
 *    the elements are the same, the order of elements in the set might be
 *    different causing the checksums to be different and hence causing the
 *    query to fail. Rewriting it to a SORT query ensures ordering and thus
 *    ensures that the checksum comparison succeeds.
 *
 * * Sets *did_rewrite='true' if a rewrite occured and 'false' if not.
 * * Does not modify 'orig_msg' and sets 'new_msg_ptr' to point to the new 'msg'
 * struct with the rewritten query if 'did_rewrite' is true.
 * * Caller must take ownership of the newly allocated msg '*new_msg_ptr'.
 */
rstatus_t redis_rewrite_query(struct msg *orig_msg, struct context *ctx,
                              bool *did_rewrite, struct msg **new_msg_ptr) {
  const char *SMEMBERS_REWRITE_FMT_STRING =
      "*3\r\n$4\r\nsort\r\n$%d\r\n%s\r\n$5\r\nalpha\r\n";

  ASSERT(orig_msg != NULL);
  ASSERT(orig_msg->is_request);
  ASSERT(did_rewrite != NULL);

  *did_rewrite = false;

  struct msg *new_msg = NULL;
  uint8_t *key = NULL;
  rstatus_t ret_status = DN_OK;

  switch (orig_msg->type) {
    case MSG_REQ_REDIS_SMEMBERS:

      if (orig_msg->owner->read_consistency == DC_SAFE_QUORUM) {

        // Get a new 'msg' structure.
        new_msg = msg_get(orig_msg->owner, true, __FUNCTION__);
        if (new_msg == NULL) {
          ret_status = DN_ENOMEM;
          goto error;
        }

        uint32_t keylen;
        // Get a copy of the key from 'orig_msg'.
        key = msg_get_full_key_copy(orig_msg, 0, &keylen);
        if (key == NULL) {
          ret_status = DN_ENOMEM;
          goto error;
        }

        // Write the new command into 'new_msg'
        rstatus_t prepend_status = msg_prepend_format(
            new_msg, SMEMBERS_REWRITE_FMT_STRING, keylen, key);
        if (prepend_status != DN_OK) {
          ret_status = prepend_status;
          goto error;
        }

        {
          // Point the 'pos' pointer in 'new_msg' to the mbuf we've added.
          struct mbuf *new_mbuf = STAILQ_LAST(&new_msg->mhdr, mbuf, next);
          new_msg->pos = new_mbuf->pos;
        }
        // Parse the message 'new_msg' to populate all of its appropriate
        // fields.
        new_msg->parser(new_msg, ctx);
        // Check if 'new_msg' was parsed successfully.
        if (new_msg->result != MSG_PARSE_OK) {
          ret_status = DN_ERROR;
          goto error;
        }

        *new_msg_ptr = new_msg;
        *did_rewrite = true;
        goto done;
      }
      break;
    default:
      return DN_OK;
  }

error:
  if (key != NULL) dn_free(key);
  // Return the newly allocated message back to the free message queue.
  if (new_msg != NULL) msg_put(new_msg);
  return ret_status;

done:
  if (key != NULL) dn_free(key);
  return DN_OK;
}

/*
 * Reference: http://redis.io/topics/protocol
 *
 * Redis >= 1.2 uses the unified protocol to send requests to the Redis
 * server. In the unified protocol all the arguments sent to the server
 * are binary safe and every request has the following general form:
 *
 *   *<number of arguments> CR LF
 *   $<number of bytes of argument 1> CR LF
 *   <argument data> CR LF
 *   ...
 *   $<number of bytes of argument N> CR LF
 *   <argument data> CR LF
 *
 * Before the unified request protocol, redis protocol for requests supported
 * the following commands
 * 1). Inline commands: simple commands where arguments are just space
 *     separated strings. No binary safeness is possible.
 * 2). Bulk commands: bulk commands are exactly like inline commands, but
 *     the last argument is handled in a special way in order to allow for
 *     a binary-safe last argument.
 * 3). Single command that contains a space. Eg: "SCRIPT LOAD"
 *
 * Dynomite supports the Redis unified protocol for requests and inline ping.
 * The inline ping is being utilized by redis-benchmark
 */
void redis_parse_req(struct msg *r, struct context *ctx) {
  struct mbuf *b;
  uint8_t *p, *m = 0;
  uint8_t ch;
  enum {
    SW_START,
    SW_NARG,
    SW_NARG_LF,
    SW_REQ_TYPE_LEN,
    SW_REQ_TYPE_LEN_LF,
    SW_REQ_TYPE,
    SW_REQ_TYPE_LF,
    SW_KEY_LEN,
    SW_KEY_LEN_LF,
    SW_KEY,
    SW_KEY_LF,
    SW_ARG1_LEN,
    SW_ARG1_LEN_LF,
    SW_ARG1,
    SW_ARG1_LF,
    SW_ARG2_LEN,
    SW_ARG2_LEN_LF,
    SW_ARG2,
    SW_ARG2_LF,
    SW_ARG3_LEN,
    SW_ARG3_LEN_LF,
    SW_ARG3,
    SW_ARG3_LF,
    SW_ARGN_LEN,
    SW_ARGN_LEN_LF,
    SW_ARGN,
    SW_ARGN_LF,
    SW_FRAGMENT,
    SW_INLINE_PING,
    SW_SENTINEL
  } state;

  const struct string* hash_tag = &ctx->pool.hash_tag;

  state = r->state;

  // Get the state of read repairs in the beginning, so that we don't risk it
  // getting changed in the middle of parsing.
  bool read_repairs_enabled = is_read_repairs_enabled();

  if (read_repairs_enabled) {
    b = STAILQ_FIRST(&r->mhdr);
    if (r->state > SW_START) {
      // If this is not the first time we're parsing the same request (because we hadn't
      // receive the entire payload yet), skip all the mbufs already parsed.
      int mbuf_idx = -1;
      while (r->latest_parsed_mbuf_idx > mbuf_idx) {
        loga("Skipping previously parsed mbuf");
        b = STAILQ_NEXT(b, next);
        ++mbuf_idx;
      }
    }
  } else {
    b = STAILQ_LAST(&r->mhdr, mbuf, next);
  }

  ASSERT(r->is_request);
  ASSERT(state >= SW_START && state < SW_SENTINEL);
  ASSERT(b != NULL);
  ASSERT(b->pos <= b->last);

  /* validate the parsing maker */
  ASSERT(r->pos != NULL);
  // ASSERT(r->pos >= b->pos && r->pos <= b->last);

  for (p = r->pos; p < b->last; p++) {
    ch = *p;

    switch (state) {
      case SW_START:

      case SW_NARG:
        if (r->token == NULL) {
          if (ch == 'p' || ch == 'P') { /* inline ping */
            state = SW_INLINE_PING;
            log_hexdump(LOG_VERB, b->pos, mbuf_length(b), "INLINE PING");
            break;
          } else if (ch != '*') {
            goto error;
          }
          r->token = p;
          /* req_start <- p */
          r->ntoken_start = p;
          r->rntokens = 0;
          state = SW_NARG;
        } else if (isdigit(ch)) {
          r->rntokens = r->rntokens * 10 + (uint32_t)(ch - '0');
        } else if (ch == CR) {
          if (r->rntokens == 0) {
            goto error;
          }
          r->ntokens = r->rntokens;
          r->ntoken_end = p;
          r->token = NULL;
          state = SW_NARG_LF;
        } else {
          goto error;
        }

        break;

      case SW_INLINE_PING:
        if (str3icmp(p, 'i', 'n', 'g') && p + 4 < b->last) {
          p = p + 4;
          log_hexdump(LOG_VERB, b->pos, mbuf_length(b), "PING");
          r->type = MSG_REQ_REDIS_PING;
          r->is_read = 1;
          state = SW_REQ_TYPE_LF;
          goto done;
        } else {
          log_hexdump(LOG_VERB, b->pos, mbuf_length(b), "PING ERROR %d, %s",
                      p - m, p);
          goto error;
        }

        break;

      case SW_NARG_LF:
        switch (ch) {
          case LF:
            state = SW_REQ_TYPE_LEN;
            break;

          default:
            goto error;
        }

        break;

      case SW_REQ_TYPE_LEN:
        if (r->token == NULL) {
          if (ch != '$') {
            goto error;
          }
          r->token = p;
          r->rlen = 0;
        } else if (isdigit(ch)) {
          r->rlen = r->rlen * 10 + (uint32_t)(ch - '0');
        } else if (ch == CR) {
          if (r->rlen == 0 || r->rntokens == 0) {
            goto error;
          }
          r->rntokens--;
          r->token = NULL;
          state = SW_REQ_TYPE_LEN_LF;
        } else {
          goto error;
        }

        break;

      case SW_REQ_TYPE_LEN_LF:
        switch (ch) {
          case LF:
            state = SW_REQ_TYPE;
            break;

          default:
            goto error;
        }

        break;

      case SW_REQ_TYPE:
        if (r->token == NULL) {
          r->token = p;
        }

        // TODO: No multi-mbuf support for SW_REQ_TYPE (since very unlikely)
        m = r->token + r->rlen;
        if (m >= b->last) {
          m = b->last - 1;
          p = m;
          break;
        }

        if (*m != CR) {
          goto error;
        }

        p = m; /* move forward by rlen bytes */
        r->rlen = 0;
        m = r->token;
        r->token = NULL;

        // 'SCRIPT' commands are parsed in 2 steps due to the whitespace in between cmds,
        // so don't set the type to MSG_UNKNOWN.
        if (r->type != MSG_REQ_REDIS_SCRIPT) r->type = MSG_UNKNOWN;

        switch (p - m) {
          case 3:
            if (str3icmp(m, 'g', 'e', 't')) {
              r->type = MSG_REQ_REDIS_GET;
              r->is_read = 1;
              break;
            }

            if (str3icmp(m, 's', 'e', 't')) {
              r->type = MSG_REQ_REDIS_SET;
              r->is_read = 0;
              break;
            }

            if (str3icmp(m, 't', 't', 'l')) {
              r->type = MSG_REQ_REDIS_TTL;
              r->is_read = 0;
              break;
            }

            if (str3icmp(m, 'd', 'e', 'l')) {
              r->type = MSG_REQ_REDIS_DEL;
              r->is_read = 0;
              break;
            }

            break;

          case 4:
            if (str4icmp(m, 'p', 't', 't', 'l')) {
              r->type = MSG_REQ_REDIS_PTTL;
              r->is_read = 1;
              break;
            }

            if (str4icmp(m, 'd', 'e', 'c', 'r')) {
              r->type = MSG_REQ_REDIS_DECR;
              r->is_read = 0;
              break;
            }

            if (str4icmp(m, 'd', 'u', 'm', 'p')) {
              r->type = MSG_REQ_REDIS_DUMP;
              r->is_read = 1;
              break;
            }

            if (str4icmp(m, 'h', 'd', 'e', 'l')) {
              r->type = MSG_REQ_REDIS_HDEL;
              r->is_read = 0;
              break;
            }

            if (str4icmp(m, 'h', 'g', 'e', 't')) {
              r->type = MSG_REQ_REDIS_HGET;
              r->is_read = 1;
              break;
            }

            if (str4icmp(m, 'h', 'l', 'e', 'n')) {
              r->type = MSG_REQ_REDIS_HLEN;
              r->is_read = 1;
              break;
            }

            if (str4icmp(m, 'h', 's', 'e', 't')) {
              r->type = MSG_REQ_REDIS_HSET;
              r->is_read = 0;
              break;
            }

            if (str4icmp(m, 'i', 'n', 'c', 'r')) {
              r->type = MSG_REQ_REDIS_INCR;
              r->is_read = 0;
              break;
            }

            if (str4icmp(m, 'k', 'e', 'y', 's')) { /* Yannis: Need to identify
                                                      how this is defined in
                                                      Redis protocol */
              r->type = MSG_REQ_REDIS_KEYS;
              r->msg_routing = ROUTING_LOCAL_NODE_ONLY;
              r->is_read = 1;
              break;
            }

            if (str4icmp(m, 'i', 'n', 'f', 'o')) {
              r->type = MSG_REQ_REDIS_INFO;
              r->msg_routing = ROUTING_LOCAL_NODE_ONLY;
              r->is_read = 1;
              break;
            }

            if (str4icmp(m, 'l', 'l', 'e', 'n')) {
              r->type = MSG_REQ_REDIS_LLEN;
              r->is_read = 1;
              break;
            }

            if (str4icmp(m, 'l', 'p', 'o', 'p')) {
              r->type = MSG_REQ_REDIS_LPOP;
              r->is_read = 0;
              break;
            }

            if (str4icmp(m, 'l', 'r', 'e', 'm')) {
              r->type = MSG_REQ_REDIS_LREM;
              r->is_read = 0;
              break;
            }

            if (str4icmp(m, 'l', 's', 'e', 't')) {
              r->type = MSG_REQ_REDIS_LSET;
              r->is_read = 0;
              break;
            }

            if (str4icmp(m, 'm', 'g', 'e', 't')) {
              r->type = MSG_REQ_REDIS_MGET;
              r->is_read = 1;
              break;
            }

            if (str4icmp(m, 'm', 's', 'e',
                         't')) { /* Yannis: need to investigate the fan out of
                                    data to multiple nodes */
              r->type = MSG_REQ_REDIS_MSET;
              r->is_read = 0;
              break;
            }

            if (str4icmp(m, 'p', 'i', 'n', 'g')) {
              r->type = MSG_REQ_REDIS_PING;
              r->msg_routing = ROUTING_LOCAL_NODE_ONLY;
              p = p + 1;
              r->is_read = 1;
              goto done;
            }

            if (str4icmp(m, 'r', 'p', 'o', 'p')) {
              r->type = MSG_REQ_REDIS_RPOP;
              r->is_read = 0;
              break;
            }

            if (str4icmp(m, 's', 'a', 'd', 'd')) {
              r->type = MSG_REQ_REDIS_SADD;
              r->is_read = 0;
              break;
            }

            if (str4icmp(m, 's', 'c', 'a', 'n')) {
              r->type = MSG_REQ_REDIS_SCAN;
              r->msg_routing = ROUTING_LOCAL_NODE_ONLY;
              r->is_read = 1;
              break;
            }

            if (str4icmp(m, 's', 'p', 'o', 'p')) {
              r->type = MSG_REQ_REDIS_SPOP;
              r->is_read = 0;
              break;
            }

            if (str4icmp(m, 's', 'r', 'e', 'm')) {
              r->type = MSG_REQ_REDIS_SREM;
              r->is_read = 0;
              break;
            }

            if (str4icmp(m, 't', 'y', 'p', 'e')) {
              r->type = MSG_REQ_REDIS_TYPE;
              r->is_read = 1;
              break;
            }

            if (str4icmp(m, 'z', 'a', 'd', 'd')) {
              r->type = MSG_REQ_REDIS_ZADD;
              r->is_read = 0;
              break;
            }

            if (str4icmp(m, 'z', 'r', 'e', 'm')) {
              r->type = MSG_REQ_REDIS_ZREM;
              r->is_read = 0;
              break;
            }

            if (str4icmp(m, 'e', 'v', 'a', 'l')) {
              r->type = MSG_REQ_REDIS_EVAL;
              r->is_read = 0;
              break;
            }

            if (str4icmp(m, 's', 'o', 'r', 't')) {
              r->type = MSG_REQ_REDIS_SORT;
              r->is_read = 1;
              break;
            }

            if (str4icmp(m, 'q', 'u', 'i', 't')) {
              r->type = MSG_REQ_REDIS_QUIT;
              r->quit = 1;
              break;
            }
            if (str4icmp(m, 'l', 'o', 'a', 'd')) {
              // A command called 'LOAD' does not exist. This is the second half of the
              // command 'SCRIPT LOAD'.
              r->type = MSG_REQ_REDIS_SCRIPT_LOAD;
              r->msg_routing = ROUTING_ALL_NODES_ALL_RACKS_ALL_DCS;
              r->is_read = 0;
              break;
            }
            if (str4icmp(m, 'k', 'i', 'l', 'l')) {
              // A command called 'KILL' does not exist. This is the second half of the
              // command 'SCRIPT KILL'.
              r->type = MSG_REQ_REDIS_SCRIPT_KILL;
              r->msg_routing = ROUTING_ALL_NODES_ALL_RACKS_ALL_DCS;
              r->is_read = 0;
              break;
            }


            break;

          case 5:
            if (str5icmp(m, 'h', 'k', 'e', 'y', 's')) {
              r->type = MSG_REQ_REDIS_HKEYS;
              r->msg_routing = ROUTING_TOKEN_OWNER_LOCAL_RACK_ONLY;
              r->is_read = 1;
              break;
            }

            if (str5icmp(m, 'h', 'm', 'g', 'e', 't')) {
              r->type = MSG_REQ_REDIS_HMGET;
              r->is_read = 1;
              break;
            }

            if (str5icmp(m, 'h', 'm', 's', 'e', 't')) {
              r->type = MSG_REQ_REDIS_HMSET;
              r->is_read = 0;
              break;
            }

            if (str5icmp(m, 'h', 'v', 'a', 'l', 's')) {
              r->type = MSG_REQ_REDIS_HVALS;
              r->msg_routing = ROUTING_TOKEN_OWNER_LOCAL_RACK_ONLY;
              r->is_read = 1;
              break;
            }

            if (str5icmp(m, 'h', 's', 'c', 'a', 'n')) {
              r->type = MSG_REQ_REDIS_HSCAN;
              r->msg_routing = ROUTING_TOKEN_OWNER_LOCAL_RACK_ONLY;
              r->is_read = 1;
              break;
            }

            if (str5icmp(m, 'l', 'p', 'u', 's', 'h')) {
              r->type = MSG_REQ_REDIS_LPUSH;
              r->is_read = 0;
              break;
            }

            if (str5icmp(m, 'l', 't', 'r', 'i', 'm')) {
              r->type = MSG_REQ_REDIS_LTRIM;
              r->is_read = 0;
              break;
            }

            if (str5icmp(m, 'r', 'p', 'u', 's', 'h')) {
              r->type = MSG_REQ_REDIS_RPUSH;
              r->is_read = 0;
              break;
            }

            if (str5icmp(m, 's', 'c', 'a', 'r', 'd')) {
              r->type = MSG_REQ_REDIS_SCARD;
              r->is_read = 1;
              break;
            }

            if (str5icmp(m, 's', 'd', 'i', 'f', 'f')) {
              r->type = MSG_REQ_REDIS_SDIFF;
              r->is_read = 1;
              break;
            }

            if (str5icmp(m, 's', 'e', 't', 'e', 'x')) {
              r->type = MSG_REQ_REDIS_SETEX;
              r->is_read = 0;
              break;
            }

            if (str5icmp(m, 's', 'e', 't', 'n', 'x')) {
              r->type = MSG_REQ_REDIS_SETNX;
              r->is_read = 0;
              break;
            }

            if (str5icmp(m, 's', 'm', 'o', 'v', 'e')) {
              r->type = MSG_REQ_REDIS_SMOVE;
              r->is_read = 0;
              break;
            }

            if (str5icmp(m, 's', 's', 'c', 'a', 'n')) {
              r->type = MSG_REQ_REDIS_SSCAN;
              r->msg_routing = ROUTING_TOKEN_OWNER_LOCAL_RACK_ONLY;
              r->is_read = 1;
              break;
            }

            if (str5icmp(m, 'z', 'c', 'a', 'r', 'd')) {
              r->type = MSG_REQ_REDIS_ZCARD;
              r->is_read = 1;
              break;
            }

            if (str5icmp(m, 'z', 'r', 'a', 'n', 'k')) {
              r->type = MSG_REQ_REDIS_ZRANK;
              r->is_read = 1;
              break;
            }

            if (str5icmp(m, 'z', 's', 'c', 'a', 'n')) {
              r->type = MSG_REQ_REDIS_ZSCAN;
              r->msg_routing = ROUTING_TOKEN_OWNER_LOCAL_RACK_ONLY;
              r->is_read = 1;
              break;
            }
            if (str5icmp(m, 'p', 'f', 'a', 'd', 'd')) {
              r->type = MSG_REQ_REDIS_PFADD;
              r->is_read = 0;
              break;
            }
            if (str5icmp(m, 'f', 'l', 'u', 's', 'h')) {
              // A command called 'FLUSH' does not exist. This is the second half of the
              // command 'SCRIPT FLUSH'.
              ASSERT(r->type == MSG_REQ_REDIS_SCRIPT);
              r->type = MSG_REQ_REDIS_SCRIPT_FLUSH;
              r->msg_routing = ROUTING_ALL_NODES_ALL_RACKS_ALL_DCS;
              r->is_read = 0;
              break;
            }

            break;

          case 6:
            if (str6icmp(m, 'a', 'p', 'p', 'e', 'n', 'd')) {
              r->type = MSG_REQ_REDIS_APPEND;
              r->is_read = 0;
              break;
            }

            if (str6icmp(m, 'd', 'e', 'c', 'r', 'b', 'y')) {
              r->type = MSG_REQ_REDIS_DECRBY;
              r->is_read = 0;
              break;
            }

            if (str6icmp(m, 'e', 'x', 'i', 's', 't', 's')
                && r->type != MSG_REQ_REDIS_SCRIPT) {
              r->type = MSG_REQ_REDIS_EXISTS;
              r->is_read = 1;
              break;
            }
            if (str6icmp(m, 'e', 'x', 'i', 's', 't', 's')
                && r->type == MSG_REQ_REDIS_SCRIPT) {
              // This is not to be confused with 'EXISTS'. This is the second half of the
              // command 'SCRIPT EXISTS'.
              r->type = MSG_REQ_REDIS_SCRIPT_EXISTS;
              r->msg_routing = ROUTING_ALL_NODES_ALL_RACKS_ALL_DCS;
              r->is_read = 1;
              break;
            }


            if (str6icmp(m, 'e', 'x', 'p', 'i', 'r', 'e')) {
              r->type = MSG_REQ_REDIS_EXPIRE;
              r->is_read = 0;
              break;
            }

            if (str6icmp(m, 'g', 'e', 't', 'b', 'i', 't')) {
              r->type = MSG_REQ_REDIS_GETBIT;
              r->is_read = 1;
              break;
            }

            if (str6icmp(m, 'g', 'e', 't', 's', 'e', 't')) {
              r->type = MSG_REQ_REDIS_GETSET;
              r->is_read = 0;
              break;
            }

            if (str6icmp(m, 'p', 's', 'e', 't', 'e', 'x')) {
              r->type = MSG_REQ_REDIS_PSETEX;
              r->is_read = 0;
              break;
            }

            if (str6icmp(m, 'h', 's', 'e', 't', 'n', 'x')) {
              r->type = MSG_REQ_REDIS_HSETNX;
              r->is_read = 0;
              break;
            }

            if (str6icmp(m, 'i', 'n', 'c', 'r', 'b', 'y')) {
              r->type = MSG_REQ_REDIS_INCRBY;
              r->is_read = 0;
              break;
            }

            if (str6icmp(m, 'l', 'i', 'n', 'd', 'e', 'x')) {
              r->type = MSG_REQ_REDIS_LINDEX;
              r->is_read = 1;
              break;
            }

            if (str6icmp(m, 'l', 'p', 'u', 's', 'h', 'x')) {
              r->type = MSG_REQ_REDIS_LPUSHX;
              r->is_read = 0;
              break;
            }

            if (str6icmp(m, 'l', 'r', 'a', 'n', 'g', 'e')) {
              r->type = MSG_REQ_REDIS_LRANGE;
              r->is_read = 1;
              break;
            }

            if (str6icmp(m, 'r', 'p', 'u', 's', 'h', 'x')) {
              r->type = MSG_REQ_REDIS_RPUSHX;
              r->is_read = 0;
              break;
            }

            if (str6icmp(m, 's', 'e', 't', 'b', 'i', 't')) {
              r->type = MSG_REQ_REDIS_SETBIT;
              r->is_read = 0;
              break;
            }

            if (str6icmp(m, 's', 'i', 'n', 't', 'e', 'r')) {
              r->type = MSG_REQ_REDIS_SINTER;
              r->is_read = 1;
              break;
            }

            if (str6icmp(m, 's', 't', 'r', 'l', 'e', 'n')) {
              r->type = MSG_REQ_REDIS_STRLEN;
              r->is_read = 1;
              break;
            }

            if (str6icmp(m, 's', 'u', 'n', 'i', 'o', 'n')) {
              r->type = MSG_REQ_REDIS_SUNION;
              r->is_read = 1;
              break;
            }

            if (str6icmp(m, 'z', 'c', 'o', 'u', 'n', 't')) {
              r->type = MSG_REQ_REDIS_ZCOUNT;
              r->is_read = 1;
              break;
            }

            if (str6icmp(m, 'z', 'r', 'a', 'n', 'g', 'e')) {
              r->type = MSG_REQ_REDIS_ZRANGE;
              r->is_read = 1;
              break;
            }

            if (str6icmp(m, 'z', 's', 'c', 'o', 'r', 'e')) {
              r->type = MSG_REQ_REDIS_ZSCORE;
              r->is_read = 1;
              break;
            }

            if (str6icmp(m, 'c', 'o', 'n', 'f', 'i', 'g')) {
              r->type = MSG_REQ_REDIS_CONFIG;
              r->msg_routing = ROUTING_LOCAL_NODE_ONLY;
              r->is_read = 1;
              break;
            }
            if (str6icmp(m, 'g', 'e', 'o', 'a', 'd', 'd')) {
              r->type = MSG_REQ_REDIS_GEOADD;
              r->is_read = 0;
              break;
            }
            if (str6icmp(m, 'g', 'e', 'o', 'p', 'o', 's')) {
              r->type = MSG_REQ_REDIS_GEOPOS;
              r->is_read = 1;
              break;
            }
            if (str6icmp(m, 'u', 'n', 'l', 'i', 'n', 'k')) {
              r->type = MSG_REQ_REDIS_UNLINK;
              r->is_read = 0;
              break;
            }
            if (str6icmp(m, 's', 'c', 'r', 'i', 'p', 't')) {
              r->type = MSG_REQ_REDIS_SCRIPT;
              r->is_read = 0;
              break;
            }
            break;

          case 7:
            if (str7icmp(m, 'p', 'e', 'r', 's', 'i', 's', 't')) {
              r->type = MSG_REQ_REDIS_PERSIST;
              r->is_read = 0;
              break;
            }

            if (str7icmp(m, 'p', 'e', 'x', 'p', 'i', 'r', 'e')) {
              r->type = MSG_REQ_REDIS_PEXPIRE;
              r->is_read = 0;
              break;
            }

            if (str7icmp(m, 'h', 'e', 'x', 'i', 's', 't', 's')) {
              r->type = MSG_REQ_REDIS_HEXISTS;
              r->is_read = 1;
              break;
            }

            if (str7icmp(m, 'h', 'g', 'e', 't', 'a', 'l', 'l')) {
              r->type = MSG_REQ_REDIS_HGETALL;
              r->msg_routing = ROUTING_TOKEN_OWNER_LOCAL_RACK_ONLY;
              r->is_read = 1;
              break;
            }

            if (str7icmp(m, 'h', 'i', 'n', 'c', 'r', 'b', 'y')) {
              r->type = MSG_REQ_REDIS_HINCRBY;
              r->is_read = 0;
              break;
            }

            if (str7icmp(m, 'l', 'i', 'n', 's', 'e', 'r', 't')) {
              r->type = MSG_REQ_REDIS_LINSERT;
              r->is_read = 0;
              break;
            }

            if (str7icmp(m, 'z', 'i', 'n', 'c', 'r', 'b', 'y')) {
              r->type = MSG_REQ_REDIS_ZINCRBY;
              r->is_read = 0;
              break;
            }

            if (str7icmp(m, 'e', 'v', 'a', 'l', 's', 'h', 'a')) {
              r->type = MSG_REQ_REDIS_EVALSHA;
              r->is_read = 0;
              break;
            }

            if (str7icmp(m, 'r', 'e', 's', 't', 'o', 'r', 'e')) {
              r->type = MSG_REQ_REDIS_RESTORE;
              r->is_read = 0;
              break;
            }

            if (str7icmp(m, 's', 'l', 'a', 'v', 'e', 'o', 'f')) {
              r->type = MSG_REQ_REDIS_SLAVEOF;
              r->msg_routing = ROUTING_LOCAL_NODE_ONLY;
              r->is_read = 0;
              break;
            }
            if (str7icmp(m, 'p', 'f', 'c', 'o', 'u', 'n', 't')) {
              r->type = MSG_REQ_REDIS_PFCOUNT;
              r->is_read = 0;
              break;
            }
            if (str7icmp(m, 'g', 'e', 'o', 'h', 'a', 's', 'h')) {
              r->type = MSG_REQ_REDIS_GEOHASH;
              r->is_read = 1;
              break;
            }
            if (str7icmp(m, 'g', 'e', 'o', 'd', 'i', 's', 't')) {
              r->type = MSG_REQ_REDIS_GEODIST;
              r->is_read = 1;
              break;
            }
            if (str7icmp(m, 'h', 's', 't', 'r', 'l', 'e', 'n')) {
              r->type = MSG_REQ_REDIS_HSTRLEN;
              r->is_read = 1;
              break;
            }

            break;

          case 8:
            if (str8icmp(m, 'e', 'x', 'p', 'i', 'r', 'e', 'a', 't')) {
              r->type = MSG_REQ_REDIS_EXPIREAT;
              r->is_read = 0;
              break;
            }

            if (str8icmp(m, 'b', 'i', 't', 'c', 'o', 'u', 'n', 't')) {
              r->type = MSG_REQ_REDIS_BITCOUNT;
              r->is_read = 1;
              break;
            }

            if (str8icmp(m, 'g', 'e', 't', 'r', 'a', 'n', 'g', 'e')) {
              r->type = MSG_REQ_REDIS_GETRANGE;
              r->is_read = 1;
              break;
            }

            if (str8icmp(m, 's', 'e', 't', 'r', 'a', 'n', 'g', 'e')) {
              r->type = MSG_REQ_REDIS_SETRANGE;
              r->is_read = 0;
              break;
            }

            if (str8icmp(m, 's', 'm', 'e', 'm', 'b', 'e', 'r', 's')) {
              r->type = MSG_REQ_REDIS_SMEMBERS;
              r->is_read = 1;
              break;
            }

            if (str8icmp(m, 'z', 'r', 'e', 'v', 'r', 'a', 'n', 'k')) {
              r->type = MSG_REQ_REDIS_ZREVRANK;
              r->is_read = 1;
              break;
            }
            if (str8icmp(m, 'j', 's', 'o', 'n', '.', 's', 'e', 't')) {
              r->type = MSG_REQ_REDIS_JSONSET;
              r->is_read = 0;
              break;
            }

            if (str8icmp(m, 'j', 's', 'o', 'n', '.', 'g', 'e', 't')) {
              r->type = MSG_REQ_REDIS_JSONGET;
              r->is_read = 1;
              break;
            }

            if (str8icmp(m, 'j', 's', 'o', 'n', '.', 'd', 'e', 'l')) {
              r->type = MSG_REQ_REDIS_JSONDEL;
              r->is_read = 0;
              break;
            }

            break;

          case 9:
            if (str9icmp(m, 'p', 'e', 'x', 'p', 'i', 'r', 'e', 'a', 't')) {
              r->type = MSG_REQ_REDIS_PEXPIREAT;
              r->is_read = 0;
              break;
            }

            if (str9icmp(m, 'r', 'p', 'o', 'p', 'l', 'p', 'u', 's', 'h')) {
              r->type = MSG_REQ_REDIS_RPOPLPUSH;
              r->is_read = 0;
              break;
            }

            if (str9icmp(m, 's', 'i', 's', 'm', 'e', 'm', 'b', 'e', 'r')) {
              r->type = MSG_REQ_REDIS_SISMEMBER;
              r->is_read = 1;
              break;
            }

            if (str9icmp(m, 'z', 'l', 'e', 'x', 'c', 'o', 'u', 'n', 't')) {
              r->type = MSG_REQ_REDIS_ZLEXCOUNT;
              r->is_read = 1;
              break;
            }

            if (str9icmp(m, 'z', 'r', 'e', 'v', 'r', 'a', 'n', 'g', 'e')) {
              r->type = MSG_REQ_REDIS_ZREVRANGE;
              r->is_read = 1;
              break;
            }

            if (str9icmp(m, 'g', 'e', 'o', 'r', 'a', 'd', 'i', 'u', 's')) {
              r->type = MSG_REQ_REDIS_GEORADIUS;
              r->is_read = 1;
              break;
            }
            if (str9icmp(m, 'j', 's', 'o', 'n', '.', 't', 'y', 'p', 'e')) {
              r->type = MSG_REQ_REDIS_JSONTYPE;
              r->is_read = 1;
              break;
            }

            if (str9icmp(m, 'j', 's', 'o', 'n', '.', 'm', 'g', 'e', 't')) {
              r->type = MSG_REQ_REDIS_JSONMGET;
              r->is_read = 1;
              break;
            }

            break;

          case 10:
            if (str10icmp(m, 's', 'd', 'i', 'f', 'f', 's', 't', 'o', 'r',
                          'e')) {
              r->type = MSG_REQ_REDIS_SDIFFSTORE;
              r->is_read = 0;
              break;
            }

            break;

          case 11:
            if (str11icmp(m, 'i', 'n', 'c', 'r', 'b', 'y', 'f', 'l', 'o', 'a',
                          't')) {
              r->type = MSG_REQ_REDIS_INCRBYFLOAT;
              r->is_read = 0;
              break;
            }

            if (str11icmp(m, 's', 'i', 'n', 't', 'e', 'r', 's', 't', 'o', 'r',
                          'e')) {
              r->type = MSG_REQ_REDIS_SINTERSTORE;
              r->is_read = 0;
              break;
            }

            if (str11icmp(m, 's', 'r', 'a', 'n', 'd', 'm', 'e', 'm', 'b', 'e',
                          'r')) {
              r->type = MSG_REQ_REDIS_SRANDMEMBER;
              r->is_read = 1;
              break;
            }

            if (str11icmp(m, 's', 'u', 'n', 'i', 'o', 'n', 's', 't', 'o', 'r',
                          'e')) {
              r->type = MSG_REQ_REDIS_SUNIONSTORE;
              r->is_read = 1;
              break;
            }

            if (str11icmp(m, 'z', 'i', 'n', 't', 'e', 'r', 's', 't', 'o', 'r',
                          'e')) {
              r->type = MSG_REQ_REDIS_ZINTERSTORE;
              r->is_read = 1;
              break;
            }

            if (str11icmp(m, 'z', 'r', 'a', 'n', 'g', 'e', 'b', 'y', 'l', 'e',
                          'x')) {
              r->type = MSG_REQ_REDIS_ZRANGEBYLEX;
              r->is_read = 1;
              break;
            }

            if (str11icmp(m, 'z', 'u', 'n', 'i', 'o', 'n', 's', 't', 'o', 'r',
                          'e')) {
              r->type = MSG_REQ_REDIS_ZUNIONSTORE;
              r->is_read = 1;
              break;
            }

            if (str11icmp(m, 'j', 's', 'o', 'n', '.', 'a', 'r', 'r', 'l', 'e',
                          'n')) {
              r->type = MSG_REQ_REDIS_JSONARRLEN;
              r->is_read = 1;
              break;
            }

            if (str11icmp(m, 'j', 's', 'o', 'n', '.', 'o', 'b', 'j', 'l', 'e',
                          'n')) {
              r->type = MSG_REQ_REDIS_JSONOBJLEN;
              r->is_read = 1;
              break;
            }

            break;

          case 12:
            if (str12icmp(m, 'h', 'i', 'n', 'c', 'r', 'b', 'y', 'f', 'l', 'o',
                          'a', 't')) {
              r->type = MSG_REQ_REDIS_HINCRBYFLOAT;
              r->is_read = 0;
              break;
            }

            if (str12icmp(m, 'j', 's', 'o', 'n', '.', 'o', 'b', 'j', 'k', 'e',
                          'y', 's')) {
              r->type = MSG_REQ_REDIS_JSONOBJKEYS;
              r->is_read = 1;
              break;
            }

            break;

          case 13:
            if (str13icmp(m, 'z', 'r', 'a', 'n', 'g', 'e', 'b', 'y', 's', 'c',
                          'o', 'r', 'e')) {
              r->type = MSG_REQ_REDIS_ZRANGEBYSCORE;
              r->is_read = 1;
              break;
            }

            break;

          case 14:
            if (str14icmp(m, 'z', 'r', 'e', 'm', 'r', 'a', 'n', 'g', 'e', 'b',
                          'y', 'l', 'e', 'x')) {
              r->type = MSG_REQ_REDIS_ZREMRANGEBYLEX;
              r->is_read = 0;
              break;
            }

            if (str14icmp(m, 'z', 'r', 'e', 'v', 'r', 'a', 'n', 'g', 'e', 'b',
                          'y', 'l', 'e', 'x')) {
              r->type = MSG_REQ_REDIS_ZREVRANGEBYLEX;
              r->is_read = 1;
              break;
            }

            if (str14icmp(m, 'j', 's', 'o', 'n', '.', 'a', 'r', 'r', 'a', 'p',
                          'p', 'e', 'n', 'd')) {
              r->type = MSG_REQ_REDIS_JSONARRAPPEND;
              r->is_read = 0;
              break;
            }

            if (str14icmp(m, 'j', 's', 'o', 'n', '.', 'a', 'r', 'r', 'i', 'n',
                          's', 'e', 'r', 't')) {
              r->type = MSG_REQ_REDIS_JSONARRINSERT;
              r->is_read = 0;
              break;
            }

            break;

          case 15:
            if (str15icmp(m, 'z', 'r', 'e', 'm', 'r', 'a', 'n', 'g', 'e', 'b',
                          'y', 'r', 'a', 'n', 'k')) {
              r->type = MSG_REQ_REDIS_ZREMRANGEBYRANK;
              r->is_read = 0;
              break;
            }

            break;

          case 16:
            if (str16icmp(m, 'z', 'r', 'e', 'm', 'r', 'a', 'n', 'g', 'e', 'b',
                          'y', 's', 'c', 'o', 'r', 'e')) {
              r->type = MSG_REQ_REDIS_ZREMRANGEBYSCORE;
              r->is_read = 0;
              break;
            }

            if (str16icmp(m, 'z', 'r', 'e', 'v', 'r', 'a', 'n', 'g', 'e', 'b',
                          'y', 's', 'c', 'o', 'r', 'e')) {
              r->type = MSG_REQ_REDIS_ZREVRANGEBYSCORE;
              r->is_read = 1;
              break;
            }

            break;

          case 17:
            if (str17icmp(m, 'g', 'e', 'o', 'r', 'a', 'd', 'i', 'u', 's', 'b',
                          'y', 'm', 'e', 'm', 'b', 'e', 'r')) {
              r->type = MSG_REQ_REDIS_GEORADIUSBYMEMBER;
              r->is_read = 1;
              break;
            }

            break;

          case 28:
            // Note: This is not a Redis command, but a dynomite configuration
            // command.
            if (dn_strcasecmp(m, "dyno_config:conn_consistency") == 0) {
              r->type = MSG_HACK_SETTING_CONN_CONSISTENCY;
              r->is_read = 0;
              break;
            }

            break;

          default:
            r->is_read = 1;
            break;
        }

        if (r->type == MSG_UNKNOWN) {
          log_error("parsed unsupported command '%.*s'", p - m, m);
          goto error;
        }

        log_debug(LOG_VERB, "parsed command '%.*s'", p - m, m);

        state = SW_REQ_TYPE_LF;
        break;

      case SW_REQ_TYPE_LF:
        if (r->type == MSG_REQ_REDIS_SCRIPT) {
          // Parsing "SCRIPT <LOAD/KILL/FLUSH/EXISTS>" is a special case since there is a
          // space between the keyword SCRIPT and the following keyword. To deal with
          // this, we reset the state machine to a previous state to allow parsing of the
          // second keyword as well.
          // Set 'state' back to 'SW_REQ_TYPE_LEN' to parse the second half of the command
          // the space in 'SCRIPT <LOAD/KILL/FLUSH/EXISTS>'.
          state = SW_REQ_TYPE_LEN;
          log_debug(LOG_VERB, "parsed partial command '%.*s'. Continuing to parse" \
              "remainaing part of command", p - m, m);
          break;
        }

        if (is_msg_type_dyno_config(r->type)) {
          // If this is a Dynomite config message, we parse it a bit differently.
          state = SW_ARG1_LEN;
          break;
        }
        switch (ch) {
          case LF:
            if (redis_argz(r) && (r->rntokens == 0)) {
              goto done;
            } else if (redis_arg_upto1(r) && r->rntokens == 0) {
              goto done;
            } else if (redis_arg_upto1(r) && r->rntokens == 1) {
              state = SW_ARG1_LEN;
            } else if (r->type == MSG_REQ_REDIS_SCRIPT_LOAD ||
                  r->type == MSG_REQ_REDIS_SCRIPT_EXISTS) {
              // TODO: Find a way to do this without special casing for SCRIPT.
              state = SW_ARG1_LEN;
            } else if (redis_argeval(r)) {
              state = SW_ARG1_LEN;
            } else {
              state = SW_KEY_LEN;
            }
            break;

          default:
            goto error;
        }

        break;

      case SW_KEY_LEN:
        if (r->token == NULL) {
          if (ch != '$') {
            goto error;
          }
          r->token = p;
          r->rlen = 0;
        } else if (isdigit(ch)) {
          r->rlen = r->rlen * 10 + (uint32_t)(ch - '0');
        } else if (ch == CR) {
          if (r->rlen == 0) {
            log_error("parsed bad req %" PRIu64
                      " of type %d with empty "
                      "key",
                      r->id, r->type);
            goto error;
          }
          if (r->rlen >= mbuf_data_size()) {
            log_error("parsed bad req %" PRIu64
                      " of type %d with key "
                      "length %d that greater than or equal to maximum"
                      " redis key length of %d",
                      r->id, r->type, r->rlen, mbuf_data_size());
            goto error;
          }
          if (r->rntokens == 0) {
            goto error;
          }
          r->rntokens--;
          r->token = NULL;
          state = SW_KEY_LEN_LF;
        } else {
          goto error;
        }

        break;

      case SW_KEY_LEN_LF:
        switch (ch) {
          case LF:
            state = SW_KEY;
            break;

          default:
            goto error;
        }

        break;

      case SW_KEY:
        if (r->token == NULL) {
          r->token = p;
        }

        // TODO: No multi-mbuf support for SW_KEY (since very unlikely)
        m = r->token + r->rlen;
        if (m >= b->last) {
          m = b->last - 1;
          p = m;
          break;
        }

        if (*m != CR) {
          goto error;
        } else {
          struct keypos *kpos;

          p = m; /* move forward by rlen bytes */
          r->rlen = 0;
          m = r->token;
          r->token = NULL;

          kpos = array_push(r->keys);
          if (kpos == NULL) {
            goto enomem;
          }
          kpos->start = kpos->tag_start = m;
          kpos->end = kpos->tag_end = p;
          if (!string_empty(hash_tag)) {
            uint8_t *tag_start, *tag_end;

            tag_start = dn_strchr(kpos->start, kpos->end, hash_tag->data[0]);
            if (tag_start != NULL) {
              tag_end = dn_strchr(tag_start + 1, kpos->end, hash_tag->data[1]);
              if (tag_end != NULL) {
                kpos->tag_start = tag_start + 1;
                kpos->tag_end = tag_end;
              }
            }
          }
          state = SW_KEY_LF;
        }

        break;

      case SW_KEY_LF:
        switch (ch) {
          case LF:
            if (redis_arg0(r)) {
              if (r->rntokens != 0) {
                goto error;
              }
              goto done;
            } else if (redis_arg1(r)) {
              if (r->rntokens != 1) {
                goto error;
              }
              state = SW_ARG1_LEN;
            } else if (redis_arg2(r)) {
              if (r->rntokens != 2) {
                goto error;
              }
              state = SW_ARG1_LEN;
            } else if (redis_arg3(r)) {
              if (r->rntokens != 3) {
                goto error;
              }
              state = SW_ARG1_LEN;
            } else if (redis_argn(r)) {
              if (r->rntokens == 0) {
                goto done;
              }
              state = SW_ARG1_LEN;
            } else if (redis_argx(r)) {
              if (r->rntokens == 0) {
                goto done;
              }
              state = SW_KEY_LEN;
            } else if (redis_argkvx(r)) {
              if (r->ntokens % 2 == 0) {
                goto error;
              }
              state = SW_ARG1_LEN;
            } else if (redis_argeval(r)) {
              r->nkeys--;
              if (r->nkeys > 0) {
                // if there are more keys pending, parse them
                state = SW_KEY_LEN;
              } else if (r->rntokens > 0) {
                // we finished parsing keys, now start with args
                state = SW_ARGN_LEN;
              } else {
                // no more args left, we are done
                goto done;
              }
            } else {
              goto error;
            }

            break;

          default:
            goto error;
        }

        break;

      case SW_ARG1_LEN:
        if (r->token == NULL) {
          if (ch != '$') {
            goto error;
          }
          r->rlen = 0;
          r->token = p;
        } else if (isdigit(ch)) {
          r->rlen = r->rlen * 10 + (uint32_t)(ch - '0');
        } else if (ch == CR) {
          if ((p - r->token) <= 1 || r->rntokens == 0) {
            goto error;
          }
          r->rntokens--;
          r->token = NULL;
          state = SW_ARG1_LEN_LF;
        } else {
          goto error;
        }

        break;

      case SW_ARG1_LEN_LF:
        switch (ch) {
          case LF:
            state = SW_ARG1;
            break;

          default:
            goto error;
        }

        break;

      case SW_ARG1:

        if (r->type == MSG_REQ_REDIS_CONFIG && !str3icmp(m, 'g', 'e', 't')) {
          log_error("Redis CONFIG command not supported '%.*s'", p - m, m);
          goto error;
        }

        m = p + r->rlen;

        if (is_msg_type_dyno_config(r->type)) {
          rstatus_t argstatus = record_arg(p, m, r->args);
          if (argstatus == DN_ERROR) {
            goto error;
          } else if (argstatus == DN_ENOMEM) {
            goto enomem;
          }

          r->result = MSG_PARSE_DYNO_CONFIG;
          return;
        }

        if (read_repairs_enabled) {
          bool arg1_across_mbufs = false;
          while (m >= b->last) {
            // 'm' has surpassed the current mbuf. Make the next mbuf current.
            int new_mbuf_offset = m - b->last;
            struct mbuf *next_mbuf;
            next_mbuf = STAILQ_NEXT(b, next);
            if (next_mbuf == NULL) break;
            arg1_across_mbufs = true;

            // Since the arg is across mbufs, we don't have logic to rewrite those with
            // timestamps.
            r->rewrite_with_ts_possible = false;

            m = next_mbuf->pos + new_mbuf_offset;
            b = next_mbuf;

            if (mbuf_full(b)) ++r->latest_parsed_mbuf_idx;
          }
          if (arg1_across_mbufs == false) {
            rstatus_t argstatus = record_arg(p , m , r->args);
            if (argstatus == DN_ERROR) {
              goto error;
            } else if (argstatus == DN_ENOMEM) {
              goto enomem;
            }
          }
        }
        if (m >= b->last) {
          // If we don't have a following mbuf, we expect a following incoming
          // buffer to have the rest of the payload.
          r->rlen -= (uint32_t)(b->last - p);
          m = b->last - 1;
          p = m;

          if (mbuf_full(b)) ++r->latest_parsed_mbuf_idx;
          break;
        }

        if (*m != CR) {
          goto error;
        }

        p = m; /* move forward by rlen bytes */
        r->rlen = 0;

        state = SW_ARG1_LF;

        break;

      case SW_ARG1_LF:
        switch (ch) {
          case LF:
            if (redis_arg_upto1(r) || redis_arg1(r)) {
              if (r->rntokens != 0) {
                goto error;
              }
              goto done;
            } else if (redis_arg2(r)) {
              if (r->rntokens != 1) {
                goto error;
              }
              state = SW_ARG2_LEN;
            } else if (redis_arg3(r)) {
              if (r->rntokens != 2) {
                goto error;
              }
              state = SW_ARG2_LEN;
            } else if (redis_argn(r)) {
              if (r->rntokens == 0) {
                goto done;
              }
              state = SW_ARGN_LEN;
            } else if (redis_argeval(r)) {
              if (r->rntokens < 2) {
                log_error("Dynomite EVAL/EVALSHA requires at least 1 key. "\
                    "Currently have %d keys", r->rntokens - 1);
                goto error;
              }
              state = SW_ARG2_LEN;
            } else if (redis_argkvx(r)) {
              if (r->rntokens == 0) {
                goto done;
              }
              state = SW_KEY_LEN;
            } else {
              goto error;
            }

            break;

          default:
            goto error;
        }

        break;

      case SW_ARG2_LEN:
        if (r->token == NULL) {
          if (ch != '$') {
            goto error;
          }
          r->rlen = 0;
          r->token = p;
        } else if (isdigit(ch)) {
          r->rlen = r->rlen * 10 + (uint32_t)(ch - '0');
        } else if (ch == CR) {
          if ((p - r->token) <= 1 || r->rntokens == 0) {
            goto error;
          }
          r->rntokens--;
          r->token = NULL;
          state = SW_ARG2_LEN_LF;
        } else {
          goto error;
        }

        break;

      case SW_ARG2_LEN_LF:
        switch (ch) {
          case LF:
            state = SW_ARG2;
            break;

          default:
            goto error;
        }

        break;

      case SW_ARG2:
        if (r->token == NULL && redis_argeval(r)) {
          /*
           * For EVAL/EVALSHA, ARG2 represents the # key/arg pairs which must
           * be tokenized and stored in contiguous memory.
           */
          r->token = p;
        }

        m = p + r->rlen;
        if (read_repairs_enabled) {
          bool arg2_across_mbufs = false;
          while (m >= b->last) {
            // 'm' has surpassed the current mbuf. Make the next mbuf current.
            int new_mbuf_offset = m - b->last;
            struct mbuf *next_mbuf;
            next_mbuf = STAILQ_NEXT(b, next);
            if (next_mbuf == NULL) break;
            arg2_across_mbufs = true;

            // Since the arg is across mbufs, we don't have logic to rewrite those with
            // timestamps.
            r->rewrite_with_ts_possible = false;

            m = next_mbuf->pos + new_mbuf_offset;
            b = next_mbuf;
            if (mbuf_full(b)) ++r->latest_parsed_mbuf_idx;
          }
          if (arg2_across_mbufs == false) {
            // TODO: Verify if this is the correct behavior for EVAL/EVALSHA
            rstatus_t argstatus = record_arg(p , m , r->args);
            if (argstatus == DN_ERROR) {
              goto error;
            } else if (argstatus == DN_ENOMEM) {
              goto enomem;
            }
          }
        }
        if (m >= b->last) {
          // If we don't have a following mbuf, we expect a following incoming
          // buffer to have the rest of the payload.
          r->rlen -= (uint32_t)(b->last - p);
          m = b->last - 1;
          p = m;

          if (mbuf_full(b)) ++r->latest_parsed_mbuf_idx;
          break;
        }

        if (*m != CR) {
          goto error;
        }

        p = m; /* move forward by rlen bytes */
        r->rlen = 0;

        if (redis_argeval(r)) {
          uint32_t nkey;
          uint8_t *chp;
          /*
           * For EVAL/EVALSHA, we need to find the integer value of this
           * argument. It tells us the number of keys in the script, and
           * we need to error out if number of keys is 0. At this point,
           * both p and m point to the end of the argument and r->token
           * points to the start.
           */
          if (p - r->token < 1) {
            goto error;
          }

          for (nkey = 0, chp = r->token; chp < p; chp++) {
            if (isdigit(*chp)) {
              nkey = nkey * 10 + (uint32_t)(*chp - '0');
            } else {
              goto error;
            }
          }

          if (nkey == 0) {
            log_error("EVAL/EVALSHA requires atleast 1 key");
            goto error;
          }
          if (r->rntokens < nkey) {
            log_error("EVAL/EVALSHA Not all keys provided: expecting %u", nkey);
            goto error;
          }
          r->nkeys = nkey;
          r->token = NULL;
        }

        state = SW_ARG2_LF;

        break;

      case SW_ARG2_LF:
        switch (ch) {
          case LF:
            if (redis_arg2(r)) {
              if (r->rntokens != 0) {
                goto error;
              }
              goto done;
            } else if (redis_arg3(r)) {
              if (r->rntokens != 1) {
                goto error;
              }
              state = SW_ARG3_LEN;
            } else if (redis_argn(r)) {
              if (r->rntokens == 0) {
                goto done;
              }
              state = SW_ARGN_LEN;
            } else if (redis_argeval(r)) {
              if (r->rntokens < 1) {
                goto error;
              }
              state = SW_KEY_LEN;
            } else {
              goto error;
            }

            break;

          default:
            goto error;
        }

        break;

      case SW_ARG3_LEN:
        if (r->token == NULL) {
          if (ch != '$') {
            goto error;
          }
          r->rlen = 0;
          r->token = p;
        } else if (isdigit(ch)) {
          r->rlen = r->rlen * 10 + (uint32_t)(ch - '0');
        } else if (ch == CR) {
          if ((p - r->token) <= 1 || r->rntokens == 0) {
            goto error;
          }
          r->rntokens--;
          r->token = NULL;
          state = SW_ARG3_LEN_LF;
        } else {
          goto error;
        }

        break;

      case SW_ARG3_LEN_LF:
        switch (ch) {
          case LF:
            state = SW_ARG3;
            break;

          default:
            goto error;
        }

        break;

      case SW_ARG3:
        m = p + r->rlen;
        if (read_repairs_enabled) {
          bool arg3_across_mbufs = false;
          while (m >= b->last) {
            // 'm' has surpassed the current mbuf. Make the next mbuf current.
            int new_mbuf_offset = m - b->last;
            struct mbuf *next_mbuf;
            next_mbuf = STAILQ_NEXT(b, next);
            if (next_mbuf == NULL) break;
            arg3_across_mbufs = true;

            // Since the arg is across mbufs, we don't have logic to rewrite those with
            // timestamps.
            r->rewrite_with_ts_possible = false;

            m = next_mbuf->pos + new_mbuf_offset;
            b = next_mbuf;
            if (mbuf_full(b)) ++r->latest_parsed_mbuf_idx;
          }
          if (arg3_across_mbufs == false) {
            rstatus_t argstatus = record_arg(p , m , r->args);
            if (argstatus == DN_ERROR) {
              goto error;
            } else if (argstatus == DN_ENOMEM) {
              goto enomem;
            }
          }
        }
        if (m >= b->last) {
          // If we don't have a following mbuf, we expect a following incoming
          // buffer to have the rest of the payload.
          r->rlen -= (uint32_t)(b->last - p);
          m = b->last - 1;
          p = m;

          if (mbuf_full(b)) ++r->latest_parsed_mbuf_idx;
          break;
        }

        if (*m != CR) {
          goto error;
        }

        p = m; /* move forward by rlen bytes */
        r->rlen = 0;
        state = SW_ARG3_LF;

        break;

      case SW_ARG3_LF:
        switch (ch) {
          case LF:
            if (redis_arg3(r)) {
              if (r->rntokens != 0) {
                goto error;
              }
              goto done;
            } else if (redis_argn(r)) {
              if (r->rntokens == 0) {
                goto done;
              }
              state = SW_ARGN_LEN;
            } else {
              goto error;
            }

            break;

          default:
            goto error;
        }

        break;

      case SW_ARGN_LEN:
        if (r->token == NULL) {
          if (ch != '$') {
            goto error;
          }
          r->rlen = 0;
          r->token = p;
        } else if (isdigit(ch)) {
          r->rlen = r->rlen * 10 + (uint32_t)(ch - '0');
        } else if (ch == CR) {
          if ((p - r->token) <= 1 || r->rntokens == 0) {
            goto error;
          }
          r->rntokens--;
          r->token = NULL;
          state = SW_ARGN_LEN_LF;
        } else {
          goto error;
        }

        break;

      case SW_ARGN_LEN_LF:
        switch (ch) {
          case LF:
            state = SW_ARGN;
            break;

          default:
            goto error;
        }

        break;

      case SW_ARGN:
        m = p + r->rlen;
        if (read_repairs_enabled) {
          bool argn_across_mbufs = false;
          while (m >= b->last) {
            // 'm' has surpassed the current mbuf. Make the next mbuf current.
            int new_mbuf_offset = m - b->last;
            struct mbuf *next_mbuf;
            next_mbuf = STAILQ_NEXT(b, next);
            if (next_mbuf == NULL) break;
            argn_across_mbufs = true;

            // Since the arg is across mbufs, we don't have logic to rewrite those with
            // timestamps.
            r->rewrite_with_ts_possible = false;

            m = next_mbuf->pos + new_mbuf_offset;
            b = next_mbuf;
            if (mbuf_full(b)) ++r->latest_parsed_mbuf_idx;
          }
          if (argn_across_mbufs == false) {
            rstatus_t argstatus = record_arg(p , m , r->args);
            if (argstatus == DN_ERROR) {
              goto error;
            } else if (argstatus == DN_ENOMEM) {
              goto enomem;
            }
          }
        }
        if (m >= b->last) {
          // If we don't have a following mbuf, we expect a following incoming
          // buffer to have the rest of the payload.
          r->rlen -= (uint32_t)(b->last - p);
          m = b->last - 1;
          p = m;

          if (mbuf_full(b)) ++r->latest_parsed_mbuf_idx;
          break;
        }

        if (*m != CR) {
          goto error;
        }

        p = m; /* move forward by rlen bytes */
        r->rlen = 0;
        state = SW_ARGN_LF;

        break;

      case SW_ARGN_LF:
        switch (ch) {
          case LF:
            if (redis_argn(r) || redis_argeval(r)) {
              if (r->rntokens == 0) {
                goto done;
              }
              state = SW_ARGN_LEN;
            } else {
              goto error;
            }

            break;

          default:
            goto error;
        }

        break;

      case SW_SENTINEL:
      default:
        NOT_REACHED();
        break;
    }
  }

  // ASSERT(p == b->last);
  r->pos = p;
  r->state = state;

  // If we have to parse again, we won't be able to write with the timestamp.
  r->rewrite_with_ts_possible = false;
  if (b->last == b->end && r->token != NULL) {
    r->pos = r->token;
    r->token = NULL;
    r->result = MSG_PARSE_REPAIR;

  } else {
    r->result = MSG_PARSE_AGAIN;
  }

  log_hexdump(LOG_VERB, b->pos, mbuf_length(b),
              "parsed req %" PRIu64
              " res %d "
              "type %d state %d rpos %d of %d",
              r->id, r->result, r->type, r->state, r->pos - b->pos,
              b->last - b->pos);
  return;

done:
  ASSERT(r->type > MSG_UNKNOWN && r->type < MSG_SENTINEL);
  r->pos = p + 1;
  ASSERT(r->pos <= b->last);
  r->state = SW_START;
  r->token = NULL;
  r->result = MSG_PARSE_OK;

  log_hexdump(LOG_VERB, b->pos, mbuf_length(b),
              "parsed req %" PRIu64
              " res %d "
              "type %d state %d rpos %d of %d",
              r->id, r->result, r->type, r->state, r->pos - b->pos,
              b->last - b->pos);
  return;

enomem:
  r->result = MSG_PARSE_ERROR;
  r->state = state;
  log_hexdump(LOG_ERR, b->pos, mbuf_length(b),
              "out of memory on parse req %" PRIu64
              " "
              "res %d type %d state %d",
              r->id, r->result, r->type, r->state);

  return;

error:
  r->result = MSG_PARSE_ERROR;
  r->state = state;
  errno = EINVAL;

  log_hexdump(LOG_WARN, b->pos, mbuf_length(b),
              "parsed bad req %" PRIu64
              " "
              "res %d type %d state %d",
              r->id, r->result, r->type, r->state);
}

/*
 * Reference: http://redis.io/topics/protocol
 *
 * Redis will reply to commands with different kinds of replies. It is
 * possible to check the kind of reply from the first byte sent by the
 * server:
 *  - with a single line reply the first byte of the reply will be "+"
 *  - with an error message the first byte of the reply will be "-"
 *  - with an integer number the first byte of the reply will be ":"
 *  - with bulk reply the first byte of the reply will be "$"
 *  - with multi-bulk reply the first byte of the reply will be "*"
 *
 * 1). Status reply (or single line reply) is in the form of a single line
 *     string starting with "+" terminated by "\r\n".
 * 2). Error reply are similar to status replies. The only difference is
 *     that the first byte is "-" instead of "+".
 * 3). Integer reply is just a CRLF terminated string representing an
 *     integer, and prefixed by a ":" byte.
 * 4). Bulk reply is used by server to return a single binary safe string.
 *     The first reply line is a "$" byte followed by the number of bytes
 *     of the actual reply, followed by CRLF, then the actual data bytes,
 *     followed by additional two bytes for the final CRLF. If the requested
 *     value does not exist the bulk reply will use the special value '-1'
 *     as the data length.
 * 5). Multi-bulk reply is used by the server to return many binary safe
 *     strings (bulks) with the initial line indicating how many bulks that
 *     will follow. The first byte of a multi bulk reply is always *.
 */
void redis_parse_rsp(struct msg *r, struct context *ctx) {
  struct mbuf *b;
  uint8_t *p, *m;
  uint8_t ch;

  enum {
    SW_START,
    SW_STATUS,
    SW_ERROR,
    SW_INTEGER,
    SW_INTEGER_START,
    SW_SIMPLE,
    SW_BULK,
    SW_BULK_LF,
    SW_BULK_ARG,
    SW_BULK_ARG_LF,
    SW_MULTIBULK,
    SW_MULTIBULK_NARG_LF,
    SW_MULTIBULK_ARGN_LEN,
    SW_MULTIBULK_ARGN_LEN_LF,
    SW_MULTIBULK_ARGN,
    SW_MULTIBULK_ARGN_LF,
    SW_RUNTO_CRLF,
    SW_ALMOST_DONE,
    SW_SENTINEL
  } state;

  state = r->state;
  b = STAILQ_LAST(&r->mhdr, mbuf, next);

  ASSERT(!r->is_request);
  ASSERT(state >= SW_START && state < SW_SENTINEL);
  ASSERT(b != NULL);
  ASSERT(b->pos <= b->last);

  /* validate the parsing marker */
  ASSERT(r->pos != NULL);
  ASSERT(r->pos >= b->pos && r->pos <= b->last);

  for (p = r->pos; p < b->last; p++) {
    ch = *p;

    switch (state) {
      case SW_START:
        r->type = MSG_UNKNOWN;
        switch (ch) {
          case '+':
            p = p - 1; /* go back by 1 byte */
            r->type = MSG_RSP_REDIS_STATUS;
            state = SW_STATUS;
            break;

          case '-':
            r->type = MSG_RSP_REDIS_ERROR;
            p = p - 1; /* go back by 1 byte */
            state = SW_ERROR;
            break;

          case ':':
            r->type = MSG_RSP_REDIS_INTEGER;
            p = p - 1; /* go back by 1 byte */
            state = SW_INTEGER;
            break;

          case '$':
            r->type = MSG_RSP_REDIS_BULK;
            p = p - 1; /* go back by 1 byte */
            state = SW_BULK;
            break;

          case '*':
            r->type = MSG_RSP_REDIS_MULTIBULK;
            p = p - 1; /* go back by 1 byte */
            state = SW_MULTIBULK;
            break;

          default:
            goto error;
        }

        break;

      case SW_STATUS:
        /* rsp_start <- p */
        state = SW_RUNTO_CRLF;
        break;

      case SW_ERROR:
        if (r->token == NULL) {
          if (ch != '-') {
            goto error;
          }
          /* rsp_start <- p */
          r->token = p;
        }
        if (ch == ' ' || ch == CR) {
          m = r->token;
          r->token = NULL;
          switch (p - m) {
            case 4:
              /*
               * -ERR no such key\r\n
               * -ERR syntax error\r\n
               * -ERR source and destination objects are the same\r\n
               * -ERR index out of range\r\n
               */
              if (str4cmp(m, '-', 'E', 'R', 'R')) {
                r->type = MSG_RSP_REDIS_ERROR_ERR;
                break;
              }

              /* -OOM command not allowed when used memory > 'maxmemory'.\r\n */
              if (str4cmp(m, '-', 'O', 'O', 'M')) {
                r->type = MSG_RSP_REDIS_ERROR_OOM;
                break;
              }

              break;

            case 5:
              /* -BUSY Redis is busy running a script. You can only call SCRIPT
               * KILL or SHUTDOWN NOSAVE.\r\n" */
              if (str5cmp(m, '-', 'B', 'U', 'S', 'Y')) {
                r->type = MSG_RSP_REDIS_ERROR_BUSY;
                break;
              }

              break;

            case 7:
              /* -NOAUTH Authentication required.\r\n */
              if (str7cmp(m, '-', 'N', 'O', 'A', 'U', 'T', 'H')) {
                r->type = MSG_RSP_REDIS_ERROR_NOAUTH;
                break;
              }

              break;

            case 8:
              /* rsp: "-LOADING Redis is loading the dataset in memory\r\n" */
              if (str8cmp(m, '-', 'L', 'O', 'A', 'D', 'I', 'N', 'G')) {
                r->type = MSG_RSP_REDIS_ERROR_LOADING;
                break;
              }

              /* -BUSYKEY Target key name already exists.\r\n */
              if (str8cmp(m, '-', 'B', 'U', 'S', 'Y', 'K', 'E', 'Y')) {
                r->type = MSG_RSP_REDIS_ERROR_BUSYKEY;
                break;
              }

              /* "-MISCONF Redis is configured to save RDB snapshots, but is
               * currently not able to persist on disk. Commands that may modify
               * the data set are disabled. Please check Redis logs for details
               * about the error.\r\n" */
              if (str8cmp(m, '-', 'M', 'I', 'S', 'C', 'O', 'N', 'F')) {
                r->type = MSG_RSP_REDIS_ERROR_MISCONF;
                break;
              }

              break;

            case 9:
              /* -NOSCRIPT No matching script. Please use EVAL.\r\n */
              if (str9cmp(m, '-', 'N', 'O', 'S', 'C', 'R', 'I', 'P', 'T')) {
                r->type = MSG_RSP_REDIS_ERROR_NOSCRIPT;
                break;
              }

              /* -READONLY You can't write against a read only slave.\r\n */
              if (str9cmp(m, '-', 'R', 'E', 'A', 'D', 'O', 'N', 'L', 'Y')) {
                r->type = MSG_RSP_REDIS_ERROR_READONLY;
                break;
              }

              break;

            case 10:
              /* -WRONGTYPE Operation against a key holding the wrong kind of
               * value\r\n */
              if (str10cmp(m, '-', 'W', 'R', 'O', 'N', 'G', 'T', 'Y', 'P',
                           'E')) {
                r->type = MSG_RSP_REDIS_ERROR_WRONGTYPE;
                break;
              }

              /* -EXECABORT Transaction discarded because of previous
               * errors.\r\n" */
              if (str10cmp(m, '-', 'E', 'X', 'E', 'C', 'A', 'B', 'O', 'R',
                           'T')) {
                r->type = MSG_RSP_REDIS_ERROR_EXECABORT;
                break;
              }

              break;

            case 11:
              /* -MASTERDOWN Link with MASTER is down and slave-serve-stale-data
               * is set to 'no'.\r\n */
              if (str11cmp(m, '-', 'M', 'A', 'S', 'T', 'E', 'R', 'D', 'O', 'W',
                           'N')) {
                r->type = MSG_RSP_REDIS_ERROR_MASTERDOWN;
                break;
              }

              /* -NOREPLICAS Not enough good slaves to write.\r\n */
              if (str11cmp(m, '-', 'N', 'O', 'R', 'E', 'P', 'L', 'I', 'C', 'A',
                           'S')) {
                r->type = MSG_RSP_REDIS_ERROR_NOREPLICAS;
                break;
              }

              break;
          }
          state = SW_RUNTO_CRLF;
        }
        break;

      case SW_INTEGER:
        /* rsp_start <- p */
        state = SW_INTEGER_START;
        r->integer = 0;
        break;

      case SW_SIMPLE:
        if (ch == CR) {
          uint8_t* j;

          // Find where this arg started.
          // TODO: Not a big deal, but avoid iterating backwards.
          for (j = p; j > 0; --j) {
            if (*j == ':' || *j == '+' || *j == '-') break;
          }

          // Record this argument.
          {
            rstatus_t argstatus = record_arg(j , p , r->args);
            if (argstatus == DN_ERROR) {
              goto error;
            } else if (argstatus == DN_ENOMEM) {
              goto enomem;
            }
          }

          state = SW_MULTIBULK_ARGN_LF;
          r->rntokens--;
        }
        break;

      case SW_INTEGER_START:
        if (ch == CR) {
          state = SW_ALMOST_DONE;
        } else if (ch == '-') {
          ;
        } else if (isdigit(ch)) {
          r->integer = r->integer * 10 + (uint32_t)(ch - '0');
        } else {
          goto error;
        }
        break;

      case SW_RUNTO_CRLF:
        switch (ch) {
          case CR:
            state = SW_ALMOST_DONE;
            break;

          default:
            break;
        }

        break;

      case SW_ALMOST_DONE:
        switch (ch) {
          case LF:
            /* rsp_end <- p */
            goto done;

          default:
            goto error;
        }

        break;

      case SW_BULK:
        if (r->token == NULL) {
          if (ch != '$') {
            goto error;
          }
          /* rsp_start <- p */
          r->token = p;
          r->rlen = 0;
        } else if (ch == '-') {
          /* handles null bulk reply = '$-1' */
          state = SW_RUNTO_CRLF;
        } else if (isdigit(ch)) {
          r->rlen = r->rlen * 10 + (uint32_t)(ch - '0');
        } else if (ch == CR) {
          if ((p - r->token) <= 1) {
            goto error;
          }
          r->token = NULL;
          state = SW_BULK_LF;
        } else {
          goto error;
        }

        break;

      case SW_BULK_LF:
        switch (ch) {
          case LF:
            state = SW_BULK_ARG;
            break;

          default:
            goto error;
        }

        break;

      case SW_BULK_ARG:
        m = p + r->rlen;
        if (m >= b->last) {
          r->rlen -= (uint32_t)(b->last - p);
          m = b->last - 1;
          p = m;
          break;
        }

        if (*m != CR) {
          goto error;
        }

        p = m; /* move forward by rlen bytes */
        r->rlen = 0;

        state = SW_BULK_ARG_LF;

        break;

      case SW_BULK_ARG_LF:
        switch (ch) {
          case LF:
            goto done;

          default:
            goto error;
        }

        break;

      case SW_MULTIBULK:
        if (r->token == NULL) {
          if (ch != '*') {
            goto error;
          }
          r->token = p;
          /* rsp_start <- p */
          r->ntoken_start = p;
          r->rntokens = 0;
        } else if (ch == '-') {
          state = SW_RUNTO_CRLF;
        } else if (isdigit(ch)) {
          r->rntokens = r->rntokens * 10 + (uint32_t)(ch - '0');
        } else if (ch == CR) {
          if ((p - r->token) <= 1) {
            goto error;
          }

          r->ntokens = r->rntokens;
          r->ntoken_end = p;
          r->token = NULL;
          state = SW_MULTIBULK_NARG_LF;
        } else {
          goto error;
        }

        break;

      case SW_MULTIBULK_NARG_LF:
        switch (ch) {
          case LF:
            if (r->rntokens == 0) {
              /* response is '*0\r\n' */
              goto done;
            }
            state = SW_MULTIBULK_ARGN_LEN;
            break;

          default:
            goto error;
        }

        break;

      case SW_MULTIBULK_ARGN_LEN:
        if (r->token == NULL) {
          /*
           * From: http://redis.io/topics/protocol, a multi bulk reply
           * is used to return an array of other replies. Every element
           * of a multi bulk reply can be of any kind, including a
           * nested multi bulk reply.
           *
           * Here, we only handle a multi bulk reply element that
           * are either integer reply or bulk reply.
           *
           * there is a special case for sscan/hscan/zscan, these command
           * replay a nested multi-bulk with a number and a multi bulk like
           * this:
           *
           * - mulit-bulk
           *    - cursor
           *    - mulit-bulk
           *       - val1
           *       - val2
           *       - val3
           *
           * in this case, there is only one sub-multi-bulk,
           * and it's the last element of parent,
           * we can handle it like tail-recursive.
           *
           */
          if (ch == '*') { /* for sscan/hscan/zscan only */
            p = p - 1;     /* go back by 1 byte */
            state = SW_MULTIBULK;
            break;
          }

          if (ch == ':' || ch == '+' || ch == '-') {
            /* handles not-found reply = '$-1' or integer reply = ':<num>' */
            /* and *2\r\n$2\r\nr0\r\n+OK\r\n or *1\r\n+OK\r\n */
            state = SW_SIMPLE;
            break;
          }

          if (ch != '$') {
            goto error;
          }

          r->token = p;
          r->rlen = 0;
        } else if (isdigit(ch)) {
          r->rlen = r->rlen * 10 + (uint32_t)(ch - '0');
        } else if (ch == '-') {
          ;
        } else if (ch == CR) {
          if ((p - r->token) <= 1 || r->rntokens == 0) {
            goto error;
          }

          if ((r->rlen == 1 && (p - r->token) == 3)) {
            r->rlen = 0;
            state = SW_MULTIBULK_ARGN_LF;
          } else {
            state = SW_MULTIBULK_ARGN_LEN_LF;
          }
          r->rntokens--;
          r->token = NULL;
        } else {
          goto error;
        }

        break;

      case SW_MULTIBULK_ARGN_LEN_LF:
        switch (ch) {
          case LF:
            state = SW_MULTIBULK_ARGN;
            break;

          default:
            goto error;
        }

        break;

      case SW_MULTIBULK_ARGN:
        m = p + r->rlen;
        if (m >= b->last) {
          r->rlen -= (uint32_t)(b->last - p);
          m = b->last - 1;
          p = m;
          break;
        }

        if (*m != CR) {
          goto error;
        }

        {
          // Record all args.
          rstatus_t argstatus = record_arg(p , m , r->args);
          if (argstatus == DN_ERROR) {
            goto error;
          } else if (argstatus == DN_ENOMEM) {
            goto enomem;
          }
        }

        p += r->rlen; /* move forward by rlen bytes */
        r->rlen = 0;

        state = SW_MULTIBULK_ARGN_LF;

        break;

      case SW_MULTIBULK_ARGN_LF:
        switch (ch) {
          case LF:
            if (r->rntokens == 0) {
              goto done;
            }

            state = SW_MULTIBULK_ARGN_LEN;
            break;

          default:
            goto error;
        }

        break;

      case SW_SENTINEL:
      default:
        NOT_REACHED();
        break;
    }
  }

  ASSERT(p == b->last);
  r->pos = p;
  r->state = state;
  r->is_error = redis_error(r);

  if (b->last == b->end && r->token != NULL) {
    r->pos = r->token;
    r->token = NULL;
    r->result = MSG_PARSE_REPAIR;
  } else {
    r->result = MSG_PARSE_AGAIN;
  }

  log_hexdump(LOG_VERB, b->pos, mbuf_length(b),
              "parsed rsp %" PRIu64
              " res %d "
              "type %d state %d rpos %d of %d",
              r->id, r->result, r->type, r->state, r->pos - b->pos,
              b->last - b->pos);
  return;

done:
  ASSERT(r->type > MSG_UNKNOWN && r->type < MSG_SENTINEL);
  r->pos = p + 1;
  ASSERT(r->pos <= b->last);
  r->state = SW_START;
  r->token = NULL;
  r->result = MSG_PARSE_OK;
  r->is_error = redis_error(r);
  log_hexdump(LOG_VERB, b->pos, mbuf_length(b),
              "parsed rsp %" PRIu64
              " res %d "
              "type %d state %d rpos %d of %d",
              r->id, r->result, r->type, r->state, r->pos - b->pos,
              b->last - b->pos);
  return;

enomem:
  r->result = MSG_PARSE_ERROR;
  r->state = state;
  log_hexdump(LOG_ERR, b->pos, mbuf_length(b),
              "out of memory on parse req %" PRIu64
              " "
              "res %d type %d state %d",
              r->id, r->result, r->type, r->state);
  return;

error:
  r->result = MSG_PARSE_ERROR;
  r->state = state;
  errno = EINVAL;

  log_hexdump(LOG_INFO, b->pos, mbuf_length(b),
              "parsed bad rsp %" PRIu64
              " "
              "res %d type %d state %d",
              r->id, r->result, r->type, r->state);
}

/*
 * copy one bulk from src to dst
 *
 * if dst == NULL, we just eat the bulk
 *
 * */
static rstatus_t redis_copy_bulk(struct msg *dst, struct msg *src, bool log) {
  struct mbuf *mbuf, *nbuf;
  uint8_t *p;
  uint32_t len = 0;
  uint32_t bytes = 0;
  rstatus_t status;

  for (mbuf = STAILQ_FIRST(&src->mhdr); mbuf && mbuf_empty(mbuf);
       mbuf = STAILQ_FIRST(&src->mhdr)) {
    mbuf_remove(&src->mhdr, mbuf);
    mbuf_put(mbuf);
  }

  mbuf = STAILQ_FIRST(&src->mhdr);
  if (mbuf == NULL) {
    return DN_ERROR;
  }

  p = mbuf->pos;
  if (*p != '$') {
    return DYNOMITE_PAYLOAD_TOO_LARGE;
  }
  p++;

  if (p[0] == '-' && p[1] == '1') {
    len = 1 + 2 + CRLF_LEN; /* $-1\r\n */
    p = mbuf->pos + len;
    if (log) log_notice("here");
  } else {
    len = 0;
    for (; p < mbuf->last && isdigit(*p); p++) {
      len = len * 10 + (uint32_t)(*p - '0');
    }
    len += CRLF_LEN * 2;
    len += (p - mbuf->pos);
  } 
  bytes = len;

  /* copy len bytes to dst */
  for (; mbuf;) {
    if (log) {
      log_notice("dumping mbuf");
      mbuf_dump(mbuf);
    }

    size_t remaining_mbuf_len = mbuf_length(mbuf);
    if (remaining_mbuf_len <= len &&
        remaining_mbuf_len == mbuf_chunk_sz()) {
      // Steal the entire buffer from src to dst if we need the whole buffer
      //
      // We only copy if it's the entire buffer because of the way our
      // encryption/decryption works. Sending out multiple partially filled
      // mbufs will fail to decrypt on the receiving side. This is because
      // in the source side we encrypt each mbuf seperately but if all the
      // partial mbufs can fit into one full mbuf on the receiving side,
      // it will do so and hence fail to decrypt.
      // TODO: Change this when the whole broken crypto scheme is changed.
      nbuf = STAILQ_NEXT(mbuf, next);
      mbuf_remove(&src->mhdr, mbuf);
      if (dst != NULL) {
        mbuf_insert(&dst->mhdr, mbuf);
      } else {
        mbuf_put(mbuf);
      }
      len -= mbuf_length(mbuf);
      mbuf = nbuf;
      if (log) log_notice("stealing mbuf");
    } else { /* split it */
      if (dst != NULL) {
        if (log) log_notice("appending mbuf");
        status = msg_append(dst, mbuf->pos, len);
        if (status != DN_OK) {
          return status;
        }
      }
      mbuf->pos += len;
      break;
    }
  }

  if (dst != NULL) {
    dst->mlen += bytes;
  }
  src->mlen -= bytes;
  log_debug(LOG_VVERB, "redis_copy_bulk copy bytes: %d", bytes);
  return DN_OK;
}

/*
 * Pre-coalesce handler is invoked when the message is a response to
 * the fragmented multi vector request - 'mget' or 'del' and all the
 * responses to the fragmented request vector hasn't been received
 */
void redis_pre_coalesce(struct msg *rsp) {
  struct msg *req = rsp->peer; /* peer request */
  struct mbuf *mbuf;

  ASSERT(!rsp->is_request);
  ASSERT(req->is_request);

  if (req->frag_id == 0) {
    /* do nothing, if not a response to a fragmented request */
    return;
  }

  req->frag_owner->nfrag_done++;
  switch (rsp->type) {
    case MSG_RSP_REDIS_INTEGER:
      /* only redis 'del' fragmented request sends back integer reply */
      ASSERT((req->type == MSG_REQ_REDIS_DEL) ||
             (req->type == MSG_REQ_REDIS_EXISTS));

      mbuf = STAILQ_FIRST(&rsp->mhdr);
      /*
       * Our response parser guarantees that the integer reply will be
       * completely encapsulated in a single mbuf and we should skip over
       * all the mbuf contents and discard it as the parser has already
       * parsed the integer reply and stored it in msg->integer
       */
      ASSERT(mbuf == STAILQ_LAST(&rsp->mhdr, mbuf, next));
      ASSERT(rsp->mlen == mbuf_length(mbuf));

      rsp->mlen -= mbuf_length(mbuf);
      mbuf_rewind(mbuf);

      /* accumulate the integer value in frag_owner of peer request */
      req->frag_owner->integer += rsp->integer;
      break;

    case MSG_RSP_REDIS_MULTIBULK:
      /* only redis 'mget' fragmented request sends back multi-bulk reply */
      ASSERT(req->type == MSG_REQ_REDIS_MGET);

      mbuf = STAILQ_FIRST(&rsp->mhdr);
      /*
       * Muti-bulk reply can span over multiple mbufs and in each reply
       * we should skip over the ntokens token. Our response parser
       * guarantees thaat the ntokens token and the immediately following
       * '\r\n' will exist in a contiguous region in the first mbuf
       */
      ASSERT(rsp->ntoken_start == mbuf->pos);
      ASSERT(rsp->ntoken_start < rsp->ntoken_end);

      rsp->ntoken_end += CRLF_LEN;
      rsp->mlen -= (uint32_t)(rsp->ntoken_end - rsp->ntoken_start);
      mbuf->pos = rsp->ntoken_end;

      break;

    case MSG_RSP_REDIS_STATUS:
      if (req->type == MSG_REQ_REDIS_MSET) { /* MSET segments */
        mbuf = STAILQ_FIRST(&rsp->mhdr);
        rsp->mlen -= mbuf_length(mbuf);
        mbuf_rewind(mbuf);
      }
      break;

    case MSG_RSP_REDIS_ERROR:
      req->is_error = rsp->is_error;
      req->error_code = rsp->error_code;
      req->dyn_error_code = rsp->dyn_error_code;
      break;

    default:
      /*
       * Valid responses for a fragmented request are MSG_RSP_REDIS_INTEGER or,
       * MSG_RSP_REDIS_MULTIBULK. For an invalid response, we send out -ERR
       * with EINVAL errno
       */
      log_warn("Invalid Response type");
      msg_dump(LOG_WARN, rsp);
      msg_dump(LOG_WARN, req);
      req->is_error = 1;
      req->error_code = EINVAL;
      break;
  }
}

/*
 * Post-coalesce handler is invoked when the message is a response to
 * the fragmented multi vector request - 'mget' or 'del' and all the
 * responses to the fragmented request vector has been received and
 * the fragmented request is consider to be done
 */
void redis_post_coalesce_mset(struct msg *request) {
  rstatus_t status;
  struct msg *response = request->selected_rsp;

  status = msg_append(response, rsp_ok.data, rsp_ok.len);
  if (status != DN_OK) {
    response->is_error = 1; /* mark this msg as err */
    response->error_code = errno;
  }
}

void redis_post_coalesce_num(struct msg *request) {
  struct msg *response = request->selected_rsp;
  rstatus_t status;

  status = msg_prepend_format(response, ":%d\r\n", request->integer);
  if (status != DN_OK) {
    response->is_error = 1;
    response->error_code = errno;
  }
}

static void redis_post_coalesce_mget(struct msg *request) {
  struct msg *response = request->selected_rsp;
  struct msg *sub_msg;
  rstatus_t status;
  uint32_t i;

  // -1 is because mget is also counted in ntokens. So the response will be 1 less
  status = msg_prepend_format(response, "*%d\r\n", request->ntokens - 1);
  if (status != DN_OK) {
    /*
     * the fragments is still in c_conn->omsg_q, we have to discard all of them,
     * we just close the conn here
     */
    log_warn("marking %s as error", print_obj(response->owner));
    response->owner->err = 1;
    return;
  }

  for (i = 0; i < array_n(request->keys); i++) {  /* for each key */
    sub_msg = request->frag_seq[i]->selected_rsp; /* get it's peer response */
    if (sub_msg == NULL) {
      struct keypos *kpos = array_get(request->keys, i);
      log_warn("Response missing for key %.*s, %s marking %s as error",
               kpos->tag_end - kpos->tag_start, kpos->tag_start,
               print_obj(request), print_obj(response->owner));
      response->owner->err = 1;
      return;
    }
    if ((sub_msg->is_error) || redis_copy_bulk(response, sub_msg, false)) {
      log_warn("marking %s as error, %s %s", print_obj(response->owner),
               print_obj(request), print_obj(response));
      msg_dump(LOG_INFO, sub_msg);
      response->owner->err = 1;
      return;
    }
  }
}

/*
 * Post-coalesce handler is invoked when the message is a response to
 * the fragmented multi vector request - 'mget' or 'del' and all the
 * responses to the fragmented request vector has been received and
 * the fragmented request is consider to be done
 */
void redis_post_coalesce(struct msg *req) {
  struct msg *rsp = req->selected_rsp; /* peer response */

  ASSERT(!rsp->is_request);
  ASSERT(req->is_request && (req->frag_owner == req));
  if (req->is_error || req->is_ferror) {
    /* do nothing, if msg is in error */
    return;
  }

  // log_notice("Post coalesce %s", print_obj(req));
  switch (req->type) {
    case MSG_REQ_REDIS_MGET:
      return redis_post_coalesce_mget(req);

    case MSG_REQ_REDIS_DEL:
    case MSG_REQ_REDIS_EXISTS:
      return redis_post_coalesce_num(req);

    case MSG_REQ_REDIS_MSET:
      return redis_post_coalesce_mset(req);

    default:
      NOT_REACHED();
  }
}

static rstatus_t redis_append_key(struct msg *r, struct keypos *kpos_src) {
  uint32_t len;
  struct mbuf *mbuf;
  uint8_t printbuf[32];
  struct keypos *kpos;

  /* 1. keylen */
  uint32_t keylen = kpos_src->end - kpos_src->start;
  uint32_t taglen = kpos_src->tag_end - kpos_src->tag_start;
  len = (uint32_t)dn_snprintf(printbuf, sizeof(printbuf), "$%d\r\n", keylen);
  mbuf = msg_ensure_mbuf(r, len);
  if (mbuf == NULL) {
    return DN_ENOMEM;
  }
  mbuf_copy(mbuf, printbuf, len);
  r->mlen += len;

  /* 2. key */
  mbuf = msg_ensure_mbuf(r, keylen);
  if (mbuf == NULL) {
    return DN_ENOMEM;
  }

  kpos = array_push(r->keys);
  if (kpos == NULL) {
    return DN_ENOMEM;
  }

  kpos->start = mbuf->last;
  kpos->tag_start = kpos->start + (kpos_src->tag_start - kpos_src->start);

  kpos->end = kpos->start + keylen;
  kpos->tag_end = kpos->tag_start + taglen;

  mbuf_copy(mbuf, kpos_src->start, keylen);
  r->mlen += keylen;

  /* 3. CRLF */
  mbuf = msg_ensure_mbuf(r, CRLF_LEN);
  if (mbuf == NULL) {
    return DN_ENOMEM;
  }
  mbuf_copy(mbuf, (uint8_t *)CRLF, CRLF_LEN);
  r->mlen += (uint32_t)CRLF_LEN;

  return DN_OK;
}

/*
 * input a msg, return a msg chain.
 * ncontinuum is the number of backend redis/memcache server
 *
 * the original msg will be fragment into at most ncontinuum fragments.
 * all the keys map to the same backend will group into one fragment.
 *
 * frag_id:
 * a unique fragment id for all fragments of the message vector. including the
 * orig msg.
 *
 * frag_owner:
 * All fragments of the message use frag_owner point to the orig msg
 *
 * frag_seq:
 * the map from each key to it's fragment, (only in the orig msg)
 *
 * For example, a message vector with 3 keys:
 *
 *     get key1 key2 key3
 *
 * suppose we have 2 backend server, and the map is:
 *
 *     key1  => backend 0
 *     key2  => backend 1
 *     key3  => backend 0
 *
 * it will fragment like this:
 *
 *   +-----------------+
 *   |  msg vector     |
 *   |(original msg)   |
 *   |key1, key2, key3 |
 *   +-----------------+
 *
 *                                             frag_owner
 *                        /--------------------------------------+
 *       frag_owner      /                                       |
 *     /-----------+    | /------------+ frag_owner              |
 *     |           |    | |            |                         |
 *     |           v    v v            |                         |
 *   +--------------------+     +---------------------+ +----+----------------+
 *   |   frag_id = 10     |     |   frag_id = 10      |     |   frag_id = 10 |
 *   |     nfrag = 3      |     |      nfrag = 0      |     |      nfrag = 0 |
 *   | frag_seq = x x x   |     |     key1, key3      |     |         key2   |
 *   +------------|-|-|---+     +---------------------+ +---------------------+
 *                | | |          ^    ^                          ^
 *                | \ \          |    |                          |
 *                |  \ ----------+    |                          |
 *                +---\---------------+                          |
 *                     ------------------------------------------+
 *
 */
static rstatus_t redis_fragment_argx(struct msg *r, struct server_pool *pool,
                                     struct rack *rack,
                                     struct msg_tqh *frag_msgq,
                                     uint32_t key_step) {
  struct mbuf *mbuf;
  struct msg **sub_msgs;
  uint32_t i;
  rstatus_t status;

  ASSERT(array_n(r->keys) == (r->ntokens - 1) / key_step);

  uint32_t total_peers = array_n(&pool->peers);
  sub_msgs = dn_zalloc(total_peers * sizeof(*sub_msgs));
  if (sub_msgs == NULL) {
    return DN_ENOMEM;
  }

  ASSERT(r->frag_seq == NULL);
  r->frag_seq = dn_alloc(array_n(r->keys) * sizeof(*r->frag_seq));
  if (r->frag_seq == NULL) {
    dn_free(sub_msgs);
    return DN_ENOMEM;
  }

  mbuf = STAILQ_FIRST(&r->mhdr);
  mbuf->pos = mbuf->start;

  /*
   * This code is based on the assumption that '*ntokens\r\n$4\r\nMGET\r\n' is
   * located in a contiguous location. This is always true because we have
   * capped our MBUF_MIN_SIZE at 512 and whenever we have multiple messages, we
   * copy the tail message into a new mbuf
   */
  for (i = 0; i < 3; i++) { /* eat *ntokens\r\n$4\r\nMGET\r\n */
    for (; *(mbuf->pos) != '\n';) {
      mbuf->pos++;
    }
    mbuf->pos++;
  }

  r->frag_id = msg_gen_frag_id();
  r->nfrag = 0;
  r->frag_owner = r;

  // Calculate number of tokens per participating peer.
  // We need to know the total number of tokens before proceeding with
  // crafting each peer's command since we're trying to fit as much as
  // possible into one MBUF and not split them for convenience.
  // TODO: This is the case because of our broken crypto scheme. Change once
  // that is fixed.
  for (i = 0; i < array_n(r->keys); i++) {
    struct keypos *kpos = array_get(r->keys, i);
    // use hash-tagged start and end for forwarding.
    uint32_t idx = dnode_peer_idx_for_key_on_rack(
        pool, rack, kpos->tag_start, kpos->tag_end - kpos->tag_start);
    if (sub_msgs[idx] == NULL) {
      sub_msgs[idx] = msg_get(r->owner, r->is_request, __FUNCTION__);
      if (sub_msgs[idx] == NULL) {
        dn_free(sub_msgs);
        return DN_ENOMEM;
      }

      // Every 'sub_msg' is counted as one fragment.
      r->nfrag++;
    }

    // One token for the key
    sub_msgs[idx]->ntokens++;

    // One token for the value (eg: for MSET)
    if (key_step != 1) sub_msgs[idx]->ntokens++;
    r->frag_seq[i] = sub_msgs[idx];

    loga("frag_seq[%d]: %x   ||  idx: %d sub_msgs[idx]: %x", i,
        r->frag_seq[i], idx, sub_msgs[idx]);

  }

  for (i = 0; i < array_n(r->keys); i++) { /* for each key */
    struct msg *sub_msg;
    struct keypos *kpos = array_get(r->keys, i);
    // use hash-tagged start and end for forwarding.
    uint32_t idx = dnode_peer_idx_for_key_on_rack(
        pool, rack, kpos->tag_start, kpos->tag_end - kpos->tag_start);

    // We already created the 'sub_msg' in the previous loop.
    ASSERT(sub_msgs[idx] != NULL);
    sub_msg = sub_msgs[idx];

    if (STAILQ_EMPTY(&sub_msg->mhdr)) {
      if (r->type == MSG_REQ_REDIS_MGET) {
        status = msg_prepend_format(sub_msg, "*%d\r\n$4\r\nmget\r\n",
                                    sub_msg->ntokens + 1);
      } else if (r->type == MSG_REQ_REDIS_DEL) {
        status = msg_prepend_format(sub_msg, "*%d\r\n$3\r\ndel\r\n",
                                    sub_msg->ntokens + 1);
      } else if (r->type == MSG_REQ_REDIS_EXISTS) {
        status = msg_prepend_format(sub_msg, "*%d\r\n$6\r\nexists\r\n",
                                    sub_msg->ntokens + 1);
      } else if (r->type == MSG_REQ_REDIS_MSET) {
        status = msg_prepend_format(sub_msg, "*%d\r\n$4\r\nmset\r\n",
                                    sub_msg->ntokens + 1);
      } else {
        NOT_REACHED();
      }
      if (status != DN_OK) {
        dn_free(sub_msgs);
        return status;
      }

      sub_msg->type = r->type;
      sub_msg->frag_id = r->frag_id;
      sub_msg->frag_owner = r->frag_owner;
      sub_msg->is_read = r->is_read;

      log_info("Fragment %d) %s", i, print_obj(sub_msg));
      TAILQ_INSERT_TAIL(frag_msgq, sub_msg, m_tqe);
    }

    status = redis_append_key(sub_msg, kpos); // Adds key to the sub_msg
    if (status != DN_OK) {
      dn_free(sub_msgs);
      return status;
    }
    if (key_step == 1) { // mget,del
      continue;
    } else {                                    // mset
      status = redis_copy_bulk(NULL, r, false); // Consumes key portion of the payload
      if (status != DN_OK) {
        dn_free(sub_msgs);
        return status;
      }

      status = redis_copy_bulk(sub_msg, r, false); // Consumes and copies value to the sub_msg fragment
      if (status != DN_OK) {
        dn_free(sub_msgs);
        return status;
      }
    }
  }
  dn_free(sub_msgs);
  return DN_OK;
}

rstatus_t redis_fragment(struct msg *r, struct server_pool *pool,
                         struct rack *rack, struct msg_tqh *frag_msgq) {
  if (1 == array_n(r->keys)) {
    return DN_OK;
  }

  switch (r->type) {
    case MSG_REQ_REDIS_MGET:
    case MSG_REQ_REDIS_DEL:
    case MSG_REQ_REDIS_EXISTS:
      return redis_fragment_argx(r, pool, rack, frag_msgq, 1);

    case MSG_REQ_REDIS_MSET:
      return redis_fragment_argx(r, pool, rack, frag_msgq, 2);

    default:
      return DN_OK;
  }
}

rstatus_t redis_verify_request(struct msg *r, struct server_pool *pool,
                               struct rack *rack) {
  if (r->type != MSG_REQ_REDIS_EVAL) return DN_OK;

  // For EVAL based commands, Dynomite wants to restrict all keys used by the
  // script belong to same node
  if (1 >= array_n(r->keys)) {
    return DN_OK;
  }
  uint32_t prev_idx = 0, i;
  for (i = 0; i < array_n(r->keys); i++) { /* for each key */
    struct keypos *kpos = array_get(r->keys, i);

    // If the keys are any of the dynomite reserved keys, skip verification for them
    // as we don't distribute them based on tokens.
    if (strncmp((char*)kpos->start, ADD_SET_STR, strlen(ADD_SET_STR)) == 0) continue;
    if (strncmp((char*)kpos->start, REM_SET_STR, strlen(REM_SET_STR)) == 0) continue;
    uint32_t idx = dnode_peer_idx_for_key_on_rack(
        pool, rack, kpos->tag_start, kpos->tag_end - kpos->tag_start);
    if (i == 0) prev_idx = idx;
    if (prev_idx != idx) {
      return DYNOMITE_SCRIPT_SPANS_NODES;
    }
  }
  return DN_OK;
}

bool redis_is_multikey_request(struct msg *req) {
  ASSERT(req->is_request);
  switch (req->type) {
    case MSG_REQ_REDIS_MGET:
    case MSG_REQ_REDIS_DEL:
    case MSG_REQ_REDIS_EXISTS:
    case MSG_REQ_REDIS_MSET:
      return true;
    default:
      return false;
  }
}

static int consume_numargs_from_response(struct msg *rsp) {
  enum { SW_START, SW_NARG, SW_NARG_LF, SW_DONE } state;
  state = SW_START;

  int narg = 0;
  struct mbuf *b = STAILQ_FIRST(&rsp->mhdr);
  // struct mbuf *b = STAILQ_LAST(&rsp->mhdr, mbuf, next);
  uint8_t *p;
  uint8_t ch;
  for (p = b->pos; p < b->last; p++) {
    ch = *p;
    switch (state) {
      case SW_START:
        if (ch != '*') {
          goto error;
        }
        log_debug(LOG_VVVERB, "SW_START -> SW_NARG");
        state = SW_NARG;
        break;

      case SW_NARG:
        if (isdigit(ch)) {
          narg = narg * 10 + (ch - '0');
        } else if (ch == CR) {
          log_debug(LOG_VVVERB, "SW_START -> SW_NARG_LF %d", narg);
          state = SW_NARG_LF;
        } else {
          goto error;
        }
        break;

      case SW_NARG_LF:
        if (ch == LF) {
          log_debug(LOG_VVVERB, "SW_NARG_LF -> SW_DONE %d", narg);
          state = SW_DONE;
        } else {
          goto error;
        }
        break;
      case SW_DONE:
        log_debug(LOG_VVERB, "SW_DONE %d", narg);
        b->pos = p;
        return narg;
    }
  }
error:
  return -1;
}

static rstatus_t consume_numargs_from_responses(struct array *responses,
                                                int *narg) {
  uint32_t iter = 0;
  *narg = -2;  // some invalid value

  while (iter < array_n(responses)) {
    // get numargs
    if (*narg == -2) {
      struct msg *rsp = *(struct msg **)array_get(responses, iter);
      *narg = consume_numargs_from_response(rsp);
    } else {
      if (*narg != consume_numargs_from_response(
                       *(struct msg **)array_get(responses, iter)))
        return DN_ERROR;
    }
    iter++;
  }
  return DN_OK;
}

static rstatus_t redis_append_nargs(struct msg *rsp, int nargs) {
  size_t len = 1 + 10 + CRLF_LEN;  // len(*<int>CRLF)
  struct mbuf *mbuf = msg_ensure_mbuf(rsp, len);
  if (!mbuf) return DN_ENOMEM;
  rsp->ntoken_start = mbuf->last;
  int n = dn_scnprintf(mbuf->last, mbuf_remaining_space(mbuf), "*%d\r\n", nargs);
  mbuf->last += n;
  rsp->ntoken_end = (rsp->ntoken_start + n - CRLF_LEN);
  rsp->mlen += (uint32_t)n;
  return DN_OK;
}

static rstatus_t get_next_response_fragment(struct msg *rsp,
                                            struct msg **fragment) {
  ASSERT(*fragment == NULL);
  *fragment = rsp_get(rsp->owner);
  if (*fragment == NULL) {
    return DN_ENOMEM;
  }
  redis_copy_bulk(*fragment, rsp, false);
  return DN_OK;
}

// Returns a quorum response.
static struct msg *redis_get_fragment_quorum(
    struct array *fragment_from_responses) {
  uint32_t total = array_n(fragment_from_responses);
  ASSERT(total <= 3);
  uint32_t checksums[MAX_REPLICAS_PER_DC];
  uint32_t fragment_iter;
  for (fragment_iter = 0; fragment_iter < total; fragment_iter++) {
    checksums[fragment_iter] = msg_payload_crc32(
        *(struct msg **)array_get(fragment_from_responses, fragment_iter));
  }
  switch (total) {
    case 2:
      if (checksums[0] == checksums[1])
        return *(struct msg **)array_get(fragment_from_responses, 0);
      else
        return NULL;
    case 3:
      if (checksums[0] == checksums[1])
        return *(struct msg **)array_get(fragment_from_responses, 0);
      if (checksums[0] == checksums[2])
        return *(struct msg **)array_get(fragment_from_responses, 0);
      if (checksums[1] == checksums[2])
        return *(struct msg **)array_get(fragment_from_responses, 1);
      return NULL;
    default:
      return NULL;
  }
}

rstatus_t free_rsp_each(void *elem) {
  struct msg *rsp = *(struct msg **)elem;
  ASSERT(rsp->object.type == OBJ_RSP);
  rsp_put(rsp);
  return DN_OK;
}

// if no quorum could be achieved, return NULL
static struct msg *redis_reconcile_multikey_responses(
    struct response_mgr *rspmgr) {
  // take the responses. get each value, and compare and return the common one
  // create a copy of the responses;

  struct array cloned_responses;
  struct array cloned_rsp_fragment_array;
  struct msg *selected_rsp = NULL;

  rstatus_t s = array_init(&cloned_responses, rspmgr->good_responses,
                           sizeof(struct msg *));
  if (s != DN_OK) goto cleanup;

  s = rspmgr_clone_responses(rspmgr, &cloned_responses);
  if (s != DN_OK) goto cleanup;

  log_info("%s cloned %d good responses", print_obj(rspmgr->msg),
           array_n(&cloned_responses));

  // if number of arguments do not match, return NULL;
  int nargs;
  s = consume_numargs_from_responses(&cloned_responses, &nargs);
  if (s != DN_OK) goto cleanup;

  log_info("numargs matched = %d", nargs);

  // create the result response
  selected_rsp = rsp_get(rspmgr->conn);
  if (!selected_rsp) {
    s = DN_ENOMEM;
    goto cleanup;
  }
  selected_rsp->expect_datastore_reply =
      rspmgr->responses[0]->expect_datastore_reply;
  selected_rsp->swallow = rspmgr->responses[0]->swallow;
  selected_rsp->type = rspmgr->responses[0]->type;

  s = redis_append_nargs(selected_rsp, nargs);
  if (s != DN_OK) goto cleanup;

  log_debug(LOG_DEBUG, "%s after appending nargs", print_obj(selected_rsp));
  msg_dump(LOG_DEBUG, selected_rsp);

  // array to hold 1 fragment from each response
  s = array_init(&cloned_rsp_fragment_array, rspmgr->good_responses,
                 sizeof(struct msg *));
  if (s != DN_OK) goto cleanup;

  // for every response fragment, try to achieve a quorum
  int arg_iter;
  for (arg_iter = 0; arg_iter < nargs; arg_iter++) {
    // carve out one fragment from each response
    uint8_t response_iter;
    for (response_iter = 0; response_iter < rspmgr->good_responses;
         response_iter++) {
      struct msg *cloned_rsp =
          *(struct msg **)array_get(&cloned_responses, response_iter);
      struct msg *cloned_rsp_fragment = NULL;
      s = get_next_response_fragment(cloned_rsp, &cloned_rsp_fragment);
      if (s != DN_OK) {
        goto cleanup;
      }
      log_debug(LOG_DEBUG, "Fragment %d of %d, from response(%d of %d) %s",
                arg_iter + 1, nargs, response_iter + 1, rspmgr->good_responses,
                print_obj(cloned_rsp_fragment));
      msg_dump(LOG_DEBUG, cloned_rsp_fragment);

      struct msg **pdst = (struct msg **)array_push(&cloned_rsp_fragment_array);
      *pdst = cloned_rsp_fragment;
    }

    // Now that we have 1 fragment from each good response, try to get a quorum
    // on them
    struct msg *quorum_fragment =
        redis_get_fragment_quorum(&cloned_rsp_fragment_array);
    if (quorum_fragment == NULL) {
      if (rspmgr->msg->consistency == DC_QUORUM) {
        log_info(
            "Fragment %d of %d, none of them match, selecting first fragment",
            arg_iter + 1, nargs);
        quorum_fragment =
            *(struct msg **)array_get(&cloned_rsp_fragment_array, 0);
      } else {
        s = DN_ERROR;
        goto cleanup;
      }
    }

    log_debug(LOG_DEBUG, "quorum fragment %s", print_obj(quorum_fragment));
    msg_dump(LOG_DEBUG, quorum_fragment);
    // Copy that fragment to the resulting response
    s = redis_copy_bulk(selected_rsp, quorum_fragment, false);
    if (s != DN_OK) {
      goto cleanup;
    }

    log_debug(LOG_DEBUG, "response now is %s", print_obj(selected_rsp));
    msg_dump(LOG_DEBUG, selected_rsp);
    // free the responses in the array
    array_each(&cloned_rsp_fragment_array, free_rsp_each);
    array_reset(&cloned_rsp_fragment_array);
  }
cleanup:
  array_each(&cloned_responses, free_rsp_each);
  array_deinit(&cloned_responses);
  array_each(&cloned_rsp_fragment_array, free_rsp_each);
  array_deinit(&cloned_rsp_fragment_array);
  if (s != DN_OK) {
    rsp_put(selected_rsp);
    selected_rsp = NULL;
  }
  return selected_rsp;
}

struct msg *redis_reconcile_responses(struct response_mgr *rspmgr) {
  struct msg *selected_rsp = NULL;
  if (redis_is_multikey_request(rspmgr->msg)) {
    selected_rsp = redis_reconcile_multikey_responses(rspmgr);
  }
  // if a quorum response was achieved, good, return that.
  if (selected_rsp != NULL) return selected_rsp;

  // No quorum was achieved.
  if (rspmgr->msg->consistency == DC_QUORUM) {
    log_info("none of the responses match, returning first");
    return rspmgr->responses[0];
  } else {
    log_info("none of the responses match, returning error");
    struct msg *rsp = msg_get_error(NULL, DYNOMITE_NO_QUORUM_ACHIEVED, 0);
    // There is a case that when 1 out of three nodes are down, the
    // response manager has 1 error response and 2 good responses.
    // We reach here when the two responses differ and we want to return
    // failed to achieve quorum. In this case, free the existing error
    // response
    if (rspmgr->err_rsp) {
      rsp_put(rspmgr->err_rsp);
    }
    rspmgr->err_rsp = rsp;
    rspmgr->error_responses++;
    return rsp;
  }
}
