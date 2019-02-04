#include "dyn_core.h"
#include "dyn_dnode_peer.h"
#include "dyn_message.h"
#include "dyn_server.h"

void init_response_mgr(struct response_mgr *rspmgr, struct msg *req,
                       bool is_read, uint8_t max_responses, struct conn *conn) {
  memset(rspmgr, 0, sizeof(struct response_mgr));
  rspmgr->is_read = is_read;
  rspmgr->max_responses = max_responses;
  rspmgr->quorum_responses = (uint8_t)(max_responses / 2 + 1);
  rspmgr->conn = conn;
  rspmgr->msg = req;
  req->awaiting_rsps = max_responses;
}

static bool rspmgr_is_quorum_achieved(struct response_mgr *rspmgr) {
  if (rspmgr->quorum_responses == 1 &&
      rspmgr->good_responses == rspmgr->quorum_responses)
    return true;
  if (rspmgr->good_responses < rspmgr->quorum_responses) return false;

  uint32_t chk0, chk1, chk2;
  chk0 = rspmgr->checksums[0];
  chk1 = rspmgr->checksums[1];
  if (chk0 == chk1) return true;
  if (rspmgr->good_responses < 3) return false;
  chk2 = rspmgr->checksums[2];
  if ((chk1 == chk2) || (chk0 == chk2)) return true;
  return false;
}

// Wait for all responses before responding
/*bool
rspmgr_check_is_done(struct response_mgr *rspmgr)
{
    uint8_t pending_responses = rspmgr->max_responses -
                                rspmgr->good_responses -
                                rspmgr->error_responses;
    if (pending_responses) {
        rspmgr->done = false;
    } else
        rspmgr->done = true;
    return rspmgr->done;
}*/

// Wait for only quorum number of responses before responding
bool rspmgr_check_is_done(struct response_mgr *rspmgr) {
  uint8_t pending_responses = (uint8_t)(
      rspmgr->max_responses - rspmgr->good_responses - rspmgr->error_responses);
  // do the required calculation and tell if we are done here
  if (rspmgr->good_responses >= rspmgr->quorum_responses) {
    // We received enough good responses but do their checksum match?
    if (rspmgr_is_quorum_achieved(rspmgr)) {
      log_info("req %lu quorum achieved", rspmgr->msg->id);
      rspmgr->done = true;
    } else if (pending_responses) {
      // There's a mismatch in checksum. Wait for any pending responses
      rspmgr->done = false;
    } else {
      // no pending responses, and the checksum do not match.
      rspmgr->done = true;
    }
  } else if ((pending_responses + rspmgr->good_responses) <
             rspmgr->quorum_responses) {
    // Even if we receive all the pending responses, still we do not form
    // a quorum, So decision is done, no quorum possible
    rspmgr->done = true;
  }
  return rspmgr->done;
}

static void rspmgr_incr_non_quorum_responses_stats(
    struct response_mgr *rspmgr) {
  if (rspmgr->is_read)
    stats_pool_incr(conn_to_ctx(rspmgr->conn), client_non_quorum_r_responses);
  else
    stats_pool_incr(conn_to_ctx(rspmgr->conn), client_non_quorum_w_responses);
}

void perform_repairs_if_necessary(struct context *ctx, struct response_mgr *rspmgr) {
  // Find the 'rsp' with the largest timestamp.
  int i;
  struct msg* most_updated_rsp = rspmgr->responses[0];
  uint64_t biggest_ts = most_updated_rsp->timestamp;
  for (i = 1 ; i < rspmgr->good_responses; ++i) {
    if (rspmgr->responses[i]->timestamp > biggest_ts) {
      most_updated_rsp = rspmgr->responses[i];
      biggest_ts = rspmgr->responses[i]->timestamp;
    }
  }

  struct conn *c_conn = rspmgr->msg->owner;
  // Mark messages in need of repair
  for (i = 0; i < rspmgr->good_responses; ++i) {
    if (biggest_ts > rspmgr->responses[i]->timestamp) {
      log_debug(LOG_VERB, "%.*s needs repair", rspmgr->responses[i]->owner->pname.len,
          rspmgr->responses[i]->owner->pname.data);
      struct keypos *key_pos = (struct keypos*)array_get(rspmgr->msg->keys, 0);
      uint32_t keylen = key_pos->end - key_pos->start;
      struct msg* repair_msg;
      rstatus_t repair_create_status = g_make_repair_query(ctx,
          rspmgr->msg->owner, biggest_ts, keylen, key_pos->start, most_updated_rsp,
          &repair_msg);
      if (repair_create_status != DN_OK) {
        log_error("Cannot create a repair message to repair %.*s",
            rspmgr->responses[i]->owner->pname.len,
            rspmgr->responses[i]->owner->pname.data);
        continue;
      }
      struct node *target_peer = (struct node*)rspmgr->responses[i]->owner->owner;
      ASSERT(target_peer != NULL);

      rstatus_t status;
      dyn_error_t dyn_error_code;

      // Forward the repair msg.
      if (target_peer->is_local == true) {
        log_debug(LOG_VERB, "Peer %.*s is local, so repairing using local path",
            rspmgr->responses[i]->owner->pname.len,
            rspmgr->responses[i]->owner->pname.data);
        status = req_forward_local_datastore(ctx, c_conn, repair_msg,
            key_pos->start, keylen, &dyn_error_code);
      } else {
        struct conn *p_conn = dnode_peer_get_conn(ctx, target_peer, c_conn->sd);
        if (p_conn == NULL) {
          log_error("Cannot get a connection to %.*s to repair",
              rspmgr->responses[i]->owner->pname.len,
              rspmgr->responses[i]->owner->pname.data);
          continue;
        }

        status = dnode_peer_req_forward(
            ctx, c_conn, p_conn, repair_msg, key_pos->start, keylen, &dyn_error_code);
        if (status == DN_OK) {
          loga("Repair msg successfully sent to %.*s",
              rspmgr->responses[i]->owner->pname.len,
              rspmgr->responses[i]->owner->pname.data);
        } else {
          log_error("Repair FAILED to send to %.*s",
              rspmgr->responses[i]->owner->pname.len,
              rspmgr->responses[i]->owner->pname.data);
        }
      }
    }
  }
}

struct msg *rspmgr_get_response(struct context *ctx, struct response_mgr *rspmgr) {
  // no quorum possible
  if (rspmgr->good_responses < rspmgr->quorum_responses) {
    ASSERT(rspmgr->err_rsp);
    rspmgr_incr_non_quorum_responses_stats(rspmgr);
    log_error("req: %lu return non quorum error rsp %p good rsp:%u quorum: %u",
              rspmgr->msg->id, rspmgr->err_rsp, rspmgr->good_responses,
              rspmgr->quorum_responses);
    msg_dump(LOG_DEBUG, rspmgr->err_rsp);
    return rspmgr->err_rsp;
  }

  perform_repairs_if_necessary(ctx, rspmgr);

  uint32_t chk0, chk1, chk2;
  chk0 = rspmgr->checksums[0];
  chk1 = rspmgr->checksums[1];
  if (chk0 == chk1) {
    return rspmgr->responses[0];
  } else if (rspmgr->good_responses == 3) {
    chk2 = rspmgr->checksums[2];
    if (chk1 == chk2)
      return rspmgr->responses[1];
    else if (chk0 == chk2)
      return rspmgr->responses[0];
  }
  rspmgr_incr_non_quorum_responses_stats(rspmgr);
  if (log_loggable(LOG_DEBUG)) {
    log_error("Request: ");
    msg_dump(LOG_DEBUG, rspmgr->msg);
  }
  if (log_loggable(LOG_VVERB)) {
    log_error("Respone 0: ");
    msg_dump(LOG_VVERB, rspmgr->responses[0]);
    log_error("Respone 1: ");
    msg_dump(LOG_VVERB, rspmgr->responses[1]);
    if (rspmgr->good_responses == 3) {
      log_error("Respone 2: ");
      msg_dump(LOG_VVERB, rspmgr->responses[2]);
    }
  }
  return g_reconcile_responses(rspmgr);
}

void rspmgr_free_other_responses(struct response_mgr *rspmgr,
                                 struct msg *dont_free) {
  int i;
  for (i = 0; i < rspmgr->good_responses; i++) {
    if (dont_free && (rspmgr->responses[i] == dont_free)) continue;
    rsp_put(rspmgr->responses[i]);
  }
  if (rspmgr->err_rsp) {
    if (dont_free && (dont_free == rspmgr->err_rsp)) return;
    rsp_put(rspmgr->err_rsp);
  }
}

rstatus_t rspmgr_submit_response(struct response_mgr *rspmgr, struct msg *rsp) {
  log_info("req %d submitting response %d awaiting_rsps %d", rspmgr->msg->id,
           rsp->id, rspmgr->msg->awaiting_rsps);
  if (rsp->is_error) {
    log_debug(LOG_VERB, "Received error response %d:%d for req %d:%d", rsp->id,
              rsp->parent_id, rspmgr->msg->id, rspmgr->msg->parent_id);
    rspmgr->error_responses++;
    if (rspmgr->err_rsp == NULL)
      rspmgr->err_rsp = rsp;
    else
      rsp_put(rsp);
  } else {
    rspmgr->checksums[rspmgr->good_responses] = msg_payload_crc32(rsp);
    log_debug(LOG_VERB, "Good response %d:%d checksum %u", rsp->id,
              rsp->parent_id, rspmgr->checksums[rspmgr->good_responses]);
    rspmgr->responses[rspmgr->good_responses++] = rsp;
  }
  msg_decr_awaiting_rsps(rspmgr->msg);
  return DN_OK;
}

rstatus_t rspmgr_clone_responses(struct response_mgr *rspmgr,
                                 struct array *responses) {
  uint8_t iter = 0;
  struct msg *dst = NULL;
  rstatus_t s = DN_OK;
  for (iter = 0; iter < rspmgr->good_responses; iter++) {
    struct msg *src = rspmgr->responses[iter];
    dst = rsp_get(rspmgr->conn);
    if (!dst) {
      s = DN_ENOMEM;
      goto error;
    }

    s = msg_clone(src, STAILQ_FIRST(&src->mhdr), dst);
    if (s != DN_OK) goto error;
    struct msg **pdst = (struct msg **)array_push(responses);
    *pdst = dst;
  }
  return DN_OK;
error:
  rsp_put(dst);
  return s;
}
