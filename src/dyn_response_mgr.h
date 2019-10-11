#ifndef _DYN_RESPONSE_MGR_H_
#define _DYN_RESPONSE_MGR_H_

#include "dyn_string.h"

#define MAX_REPLICAS_PER_DC 3
struct response_mgr {
  bool is_read;
  bool done;
  /* we could use the dynamic array
     here. But we have only 3 ASGs */
  struct msg *responses[MAX_REPLICAS_PER_DC];
  uint32_t checksums[MAX_REPLICAS_PER_DC];
  // Number of non-error responses received. (nil) is not an error.
  uint8_t good_responses;
  // Maximum number of responses possible.
  uint8_t max_responses;
  // Number of responses required to form a quorum.
  uint8_t quorum_responses;
  // Number of error responses received.
  uint8_t error_responses;
  // First error response
  struct msg *err_rsp;
  struct conn *conn;
  // Corresponding request
  struct msg *msg;
  // The DC that this response manager is responsible for.
  struct string dc_name;
};

rstatus_t init_response_mgr_all_dcs(struct context *ctx, struct msg *req,
    struct conn *c_conn, struct datacenter *local_dc);

void init_response_mgr(struct msg *req, struct response_mgr *rspmgr,
    uint8_t max_responses_for_dc, struct conn *c_conn);

// DN_OK if response was accepted
rstatus_t rspmgr_submit_response(struct response_mgr *rspmgr, struct msg *rsp);
bool rspmgr_check_is_done(struct response_mgr *rspmgr);
struct msg *rspmgr_get_response(struct context *ctx, struct response_mgr *rspmgr);
void rspmgr_free_response(struct response_mgr *rspmgr, struct msg *dont_free);
void rspmgr_free_other_responses(struct response_mgr *rspmgr,
                                 struct msg *dont_free);
rstatus_t msg_local_one_rsp_handler(struct context *ctx, struct msg *req, struct msg *rsp);
rstatus_t rspmgr_clone_responses(struct response_mgr *src,
                                 struct array *responses);

#endif
