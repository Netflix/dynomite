
#ifndef _DYN_RESPONSE_MGR_H_
#define _DYN_RESPONSE_MGR_H_
#define MAX_REPLICAS_PER_DC           3
struct response_mgr {
    bool        is_read;
    bool        done;
    /* we could use the dynamic array
       here. But we have only 3 ASGs */
    struct msg  *responses[MAX_REPLICAS_PER_DC];
    uint8_t     good_responses;     // non-error responses received
    uint8_t     max_responses;      // max responses expected
    uint8_t     quorum_responses;   // responses expected to form a quorum
    uint8_t     error_responses;    // error responses received
    struct msg  *err_rsp;           // first error response
    struct conn *conn;
    struct msg *msg;
};

void init_response_mgr(struct response_mgr *rspmgr, struct msg*, bool is_read,
                       uint8_t max_responses, struct conn *conn);
// DN_OK if response was accepted
rstatus_t rspmgr_submit_response(struct response_mgr *rspmgr, struct msg *rsp);
bool rspmgr_check_is_done(struct response_mgr *rspmgr);
struct msg* rspmgr_get_response(struct response_mgr *rspmgr);
void rspmgr_free_response(struct response_mgr *rspmgr, struct msg *dont_free);
void rspmgr_free_other_responses(struct response_mgr *rspmgr, struct msg *dont_free);


#endif
