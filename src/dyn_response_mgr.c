#include "dyn_client.h"
#include "dyn_conf.h"
#include "dyn_core.h"
#include "dyn_dnode_peer.h"
#include "dyn_message.h"
#include "dyn_server.h"


int init_response_mgr_each_quorum_helper(struct msg *req,
    struct response_mgr *rspmgr, struct conn *c_conn, struct datacenter *dc);

rstatus_t init_response_mgr_all_dcs(struct context *ctx, struct msg *req,
    struct conn *c_conn, struct datacenter *local_dc) {

  ASSERT(req->consistency == DC_EACH_SAFE_QUORUM);


  uint32_t total_responses_to_await = 0;
  int num_dcs_in_quorum = array_n(&ctx->pool.datacenters);
  req->additional_each_rspmgrs =
      (struct response_mgr**) dn_alloc(num_dcs_in_quorum * (sizeof(struct response_mgr*)));
  if (req->additional_each_rspmgrs == NULL) return DN_ENOMEM;

  // Initialize the response managers for all remote DCs. (The 0th idx in the
  // 'additional_each_rspmgrs' array is reserved for the local DC).
  int i;
  for (i = 1; i < num_dcs_in_quorum; ++i) {
    req->additional_each_rspmgrs[i] = (struct rspmgr*) dn_alloc(sizeof(struct response_mgr));
    if (req->additional_each_rspmgrs[i] == NULL) {
      goto enomem;
    }

    uint32_t dc_idx = 0;
    struct datacenter *remote_dc = NULL;
    do {
      remote_dc = (struct datacenter*) array_get(&ctx->pool.datacenters, dc_idx);
      ++dc_idx;
    } while(string_compare(remote_dc->name, local_dc->name) == 0); // Skip the local DC.

    int max_rsps_for_dc = init_response_mgr_each_quorum_helper(
        req, req->additional_each_rspmgrs[i], c_conn, remote_dc);
    if (max_rsps_for_dc == -1) goto enomem;

    total_responses_to_await += max_rsps_for_dc;
  }

  // Now do the same as the above for the local DC.
  // Point the 0th index to the statically allocated response_mgr.
  req->additional_each_rspmgrs[0] = &req->rspmgr;

  int max_rsps_for_dc = init_response_mgr_each_quorum_helper(
      req, req->additional_each_rspmgrs[0], c_conn, local_dc);
  if (max_rsps_for_dc == -1) goto enomem;
  total_responses_to_await += max_rsps_for_dc;

  // Update the total number of responses to wait for.
  req->awaiting_rsps = total_responses_to_await;

  return DN_OK;
 enomem: ;
  int j = 1;
  while (req->additional_each_rspmgrs[j] != NULL) {
    if (req->additional_each_rspmgrs[j]->dc_name.data != NULL) {
      string_deinit(&req->additional_each_rspmgrs[j]->dc_name);
    }
    dn_free(req->additional_each_rspmgrs[j]);
    ++j;
  }
  dn_free(req->additional_each_rspmgrs);
  return DN_ENOMEM;
}


/*
 * Helper function that initializes a 'struct response_mgr' and copies the appropriate
 * DC name to it. Only used under DC_EACH_SAFE_QUORUM.
 *
 * Returns maximum number of responses possible for 'dc'.
 * Returns -1 on an error.
 */
int init_response_mgr_each_quorum_helper(struct msg *req,
    struct response_mgr *rspmgr, struct conn *c_conn, struct datacenter *dc) {

  ASSERT(req->consistency == DC_EACH_SAFE_QUORUM);

  uint8_t rack_cnt = (uint8_t) array_n(&dc->racks);

  // Initialize the response mgr.
  init_response_mgr(req, rspmgr, rack_cnt, c_conn);

  // Copy the name of the DC to the response manager.
  if (string_duplicate(&rspmgr->dc_name, dc->name) != DN_OK) {
    return -1;
  }

  return rspmgr->max_responses;
}

void init_response_mgr(struct msg *req, struct response_mgr *rspmgr,
    uint8_t max_responses_for_dc, struct conn *c_conn) {

  memset(rspmgr, 0, sizeof(struct response_mgr));
  rspmgr->is_read = req->is_read;
  rspmgr->max_responses = max_responses_for_dc;
  rspmgr->quorum_responses = (uint8_t)(max_responses_for_dc / 2 + 1);
  rspmgr->conn = c_conn;
  rspmgr->msg = req;
  req->awaiting_rsps = max_responses_for_dc;

  req->rspmgrs_inited = true;
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

/*
 * Finds the most updated response based on timestamp if it exists, and repairs
 * the node(s) with outdated response(s).
 *
 * Returns 'true' if repairs were performed, 'false' otherwise.
 */
bool perform_repairs_if_necessary(struct context *ctx, struct response_mgr *rspmgr) {

  struct msg* repair_msg;
  rstatus_t repair_create_status = g_make_repair_query(ctx, rspmgr, &repair_msg);

  if (repair_create_status == DN_OK && repair_msg != NULL) {
    struct conn *c_conn = rspmgr->msg->owner;
    struct keypos *key_pos = (struct keypos*)array_get(rspmgr->msg->keys, 0);
    uint32_t keylen = key_pos->end - key_pos->start;
    int i;
    for (i = 0; i < rspmgr->good_responses; ++i) {
      if (rspmgr->responses[i]->needs_repair == false) continue;

      // Find the information of the node that needs repairing.
      struct node *target_peer = (struct node*)rspmgr->responses[i]->owner->owner;
      ASSERT(target_peer != NULL);

      {
        // (struct msg)->owner->owner can be of type (struct node*) or (struct server_pool*)
        // Responses received usually are of type (struct node) as seen in the previous
        // line, and request messages usually have a (struct server_pool)
        // (TODO: Confirm if always true)
        struct server_pool *cur_node_sp = (struct server_pool*)rspmgr->msg->owner->owner;
        int dynomite_port = cur_node_sp->proxy_endpoint.port;
        int dynomite_peer_port = cur_node_sp->dnode_proxy_endpoint.port;

        // TODO: This is a hack. A 'struct node' that points to the local datastore as
        // opposed to a dynomite peer, does not have the 'is_local' field filled in. We fill
        // it in here if we find that the port does not match the dynomite port.
        // We also would receive a response from a single datastore directly, i.e. the local
        // datastore, so it's okay to assume that is_local will be true if the ports don't
        // match.
        if (target_peer->endpoint.port != dynomite_port &&
            target_peer->endpoint.port != dynomite_peer_port) {
          target_peer->is_local = true;
        }
      }

      rstatus_t status;
      dyn_error_t dyn_error_code;

      // need to capture the initial mbuf location as once we add in the dynomite
      // headers (as mbufs to the src req), that will bork the request sent to
      // secondary racks
      struct mbuf *orig_mbuf = STAILQ_FIRST(&repair_msg->mhdr);

      // Send the repair 'msg' to the peer node.
      status = req_forward_to_peer(ctx, c_conn, repair_msg, target_peer,
          key_pos->start, keylen, orig_mbuf, true /*force copy? */,
          false /* force swallow? */, &dyn_error_code);

      IGNORE_RET_VAL(status);
    }
    return true;
  }
  return false;
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

  if (perform_repairs_if_necessary(ctx, rspmgr) == true) {
    // If the above call returns 'true', then repairs were performed. So we pick out
    // a response that was not one of the repair candidates, or rather we pick a response
    // that is the latest based on timestamp.
    int i;
    for (i = 0; i < rspmgr->good_responses; ++i) {
      if (rspmgr->responses[i]->needs_repair == false) {
        return rspmgr->responses[i];
      }
    }
  }

  // If we did not perform any repairs. we fall back to checksum matching.
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
