#include <dyn_msg_tmo.h>
#include <dyn_dnode_peer.h>

uint8_t g_timeout_factor = 1;
static struct msg *
msg_from_rbe(struct rbnode *node)
{
    struct msg *msg;
    int offset;

    offset = offsetof(struct msg, tmo_rbe);
    msg = (struct msg *)((char *)node - offset);

    return msg;
}

struct msg *
msg_tmo_min(struct msg_tmo *tmo)
{
    struct rbnode *node;

    node = rbtree_min(&tmo->tmo_rbt);
    if (node == NULL) {
        return NULL;
    }

    return msg_from_rbe(node);
}

void
msg_tmo_insert(struct msg_tmo *tmo, struct conn *conn, struct msg *msg)
{
    struct rbnode *node;
    msec_t timeout;

    //ASSERT(msg->request);
    ASSERT(!msg->quit && msg->expect_datastore_reply);

    timeout = conn->dyn_mode? dnode_peer_timeout(msg, conn) : server_timeout(conn);
    if (timeout <= 0) {
        return;
    }
    timeout = timeout * g_timeout_factor;

    node = &msg->tmo_rbe;
    node->timeout = timeout;
    node->key = dn_msec_now() + timeout;
    node->data = conn;

    rbtree_insert(&tmo->tmo_rbt, node);

    if (log_loggable(LOG_VERB)) {
       log_debug(LOG_VERB, "insert msg %"PRIu64" into tmo rbt with expiry of "
              "%d msec", msg->id, timeout);
    }
}

void
msg_tmo_delete(struct msg_tmo *tmo, struct msg *msg)
{
    struct rbnode *node;

    node = &msg->tmo_rbe;

    /* already deleted */

    if (node->data == NULL) {
        return;
    }

    rbtree_delete(&tmo->tmo_rbt, node);

    if (log_loggable(LOG_VERB)) {
       log_debug(LOG_VERB, "delete msg %"PRIu64" from tmo rbt", msg->id);
    }
}


void
msg_tmo_init(struct msg_tmo *tmo, struct thread_ctx *ptctx)
{
    rbtree_init(&tmo->tmo_rbt, &tmo->tmo_rbs);
    tmo->ptctx = ptctx;
}

void
msg_tmo_deinit(struct msg_tmo *tmo, struct thread_ctx *ptctx)
{
    //SHAILESH: TODO
}
