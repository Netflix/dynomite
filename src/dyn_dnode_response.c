/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 

#include "dyn_core.h"
#include "dyn_dnode_peer.h"


struct msg *
dnode_rsp_get(struct conn *conn)
{
	struct msg *msg;

	ASSERT(!conn->dnode_client && !conn->dnode_server);

	msg = msg_get(conn, false, conn->redis);
	if (msg == NULL) {
		conn->err = errno;
	}

	return msg;
}

void
dnode_rsp_put(struct msg *msg)
{
	ASSERT(!msg->request);
	ASSERT(msg->peer == NULL);
	msg_put(msg);
}


struct msg *
dnode_rsp_recv_next(struct context *ctx, struct conn *conn, bool alloc)
{
	ASSERT(!conn->dnode_client && !conn->dnode_server);
	return rsp_recv_next(ctx, conn, alloc);
}

static bool
dnode_rsp_filter(struct context *ctx, struct conn *conn, struct msg *msg)
{
	struct msg *pmsg;

	ASSERT(!conn->dnode_client && !conn->dnode_server);

	if (msg_empty(msg)) {
		ASSERT(conn->rmsg == NULL);
		log_debug(LOG_VERB, "dyn: filter empty rsp %"PRIu64" on s %d", msg->id,
				conn->sd);
		dnode_rsp_put(msg);
		return true;
	}

	pmsg = TAILQ_FIRST(&conn->omsg_q);
	if (pmsg == NULL) {
		log_debug(LOG_ERR, "dyn: filter stray rsp %"PRIu64" len %"PRIu32" on s %d noreply %d",
				msg->id, msg->mlen, conn->sd, msg->noreply);
		dnode_rsp_put(msg);
		return true;
	}
	ASSERT(pmsg->peer == NULL);
	ASSERT(pmsg->request && !pmsg->done);

	if (pmsg->swallow) {
		conn->dequeue_outq(ctx, conn, pmsg);
		pmsg->done = 1;

		log_debug(LOG_INFO, "dyn: swallow rsp %"PRIu64" len %"PRIu32" of req "
				"%"PRIu64" on s %d", msg->id, msg->mlen, pmsg->id,
				conn->sd);

		dnode_rsp_put(msg);
		req_put(pmsg);
		return true;
	}

	return false;
}

static void
dnode_rsp_forward_stats(struct context *ctx, struct server *server, struct msg *msg)
{
	ASSERT(!msg->request);
	stats_pool_incr(ctx, server->owner, peer_responses);
	stats_pool_incr_by(ctx, server->owner, peer_response_bytes, msg->mlen);
}


/* Description: link data from a peer connection to a client-facing connection
 * peer_conn: a peer connection
 * msg      : msg with data from the peer connection after parsing
 */
static void
dnode_rsp_forward(struct context *ctx, struct conn *peer_conn, struct msg *msg)
{
	rstatus_t status;
	struct msg *pmsg;
	struct conn *c_conn;

	loga("dnode_rsp_forward 22222222222222222222222222222222222222222222");
	ASSERT(!peer_conn->dnode_client && !peer_conn->dnode_server);

	/* response from a peer implies that peer is ok and heartbeating */
	dnode_peer_ok(ctx, peer_conn);

	/* dequeue peer message (request) from peer conn */
	pmsg = TAILQ_FIRST(&peer_conn->omsg_q);
	ASSERT(pmsg != NULL && pmsg->peer == NULL);
	ASSERT(pmsg->request && !pmsg->done);

	loga("Dumping content 2 for msg:   ");
	msg_dump(msg);

	loga("Dumping content 2 for pmsg :");
	msg_dump(pmsg);


	peer_conn->dequeue_outq(ctx, peer_conn, pmsg);
	pmsg->done = 1;

	/* establish msg <-> pmsg (response <-> request) link */
	pmsg->peer = msg;
	msg->peer = pmsg;

	msg->pre_coalesce(msg);

	//add messsage
	//struct mbuf *nbuf = mbuf_get();
	//if (nbuf == NULL) {
	//	log_debug(LOG_ERR, "Error happened in calling mbuf_get");
	//	return;  //TODOs: need to address this further
	//}

	//uint64_t msg_id = pmsg->dmsg->id;
	//uint8_t type = DMSG_RES;
	//uint8_t version = VERSION_10;

	//dmsg_write(nbuf, msg_id, type, version, peer_conn);
	//mbuf_insert_head(&pmsg->mhdr, nbuf);


	c_conn = pmsg->owner;
	ASSERT(c_conn->client && !c_conn->proxy);

	if (TAILQ_FIRST(&c_conn->omsg_q) != NULL && dnode_req_done(c_conn, TAILQ_FIRST(&c_conn->omsg_q))) {
		status = event_add_out(ctx->evb, c_conn);
		if (status != DN_OK) {
			c_conn->err = errno;
		}
	}

	dnode_rsp_forward_stats(ctx, peer_conn->owner, msg);
}



//TODOs: fix this in using dmsg_write with encrypted msgs
//         It is not in use now.
void
dnode_rsp_gos_syn(struct context *ctx, struct conn *p_conn, struct msg *msg)
{
	rstatus_t status;
	struct msg *pmsg;

	//ASSERT(p_conn->dnode_client && !p_conn->dnode_server);

	//add messsage
	struct mbuf *nbuf = mbuf_get();
	if (nbuf == NULL) {
		log_debug(LOG_ERR, "Error happened in calling mbuf_get");
		return;  //TODOs: need to address this further
	}

	msg->done = 1;

	//TODOs: need to free the old msg object
	pmsg = msg_get(p_conn, 0, msg->redis);
	if (pmsg == NULL) {
		mbuf_put(nbuf);
		return;
	}

	pmsg->done = 1;
	/* establish msg <-> pmsg (response <-> request) link */
	msg->peer = pmsg;
	pmsg->peer = msg;
	pmsg->pre_coalesce(pmsg);
	pmsg->owner = p_conn;

	//dyn message's meta data
	uint64_t msg_id = msg->dmsg->id;
	uint8_t type = GOSSIP_SYN_REPLY;
	struct string data = string("SYN_REPLY_OK");

	dmsg_write(nbuf, msg_id, type, p_conn);
	mbuf_insert(&pmsg->mhdr, nbuf);

	//dmsg_write(nbuf, msg_id, type, version, &data);
	//mbuf_insert(&pmsg->mhdr, nbuf);

	//dnode_rsp_recv_done(ctx, p_conn, msg, pmsg);
	//should we do this?
	//s_conn->dequeue_outq(ctx, s_conn, pmsg);


	/*
     p_conn->enqueue_outq(ctx, p_conn, pmsg);
     if (TAILQ_FIRST(&p_conn->omsg_q) != NULL && dnode_req_done(p_conn, TAILQ_FIRST(&p_conn->omsg_q))) {
        status = event_add_out(ctx->evb, p_conn);
        if (status != DN_OK) {
           p_conn->err = errno;
        }
     }
	 */

	if (TAILQ_FIRST(&p_conn->omsg_q) != NULL && dnode_req_done(p_conn, TAILQ_FIRST(&p_conn->omsg_q))) {
		status = event_add_out(ctx->evb, p_conn);
		if (status != DN_OK) {
			p_conn->err = errno;
		}
	}

	//dnode_rsp_forward_stats(ctx, s_conn->owner, msg);
}



void
dnode_rsp_recv_done(struct context *ctx, struct conn *conn,
		            struct msg *msg, struct msg *nmsg)
{
	loga("dnode_rsp_recv_done 5555555555555555555555555555555555555555555555");

	ASSERT(!conn->dnode_client && !conn->dnode_server);
	ASSERT(msg != NULL && conn->rmsg == msg);
	ASSERT(!msg->request);
	ASSERT(msg->owner == conn);
	ASSERT(nmsg == NULL || !nmsg->request);

	loga("Dumping content for msg:   ");
	msg_dump(msg);

	if (nmsg != NULL) {
	   loga("Dumping content for nmsg :");
	   msg_dump(nmsg);
	}

	/* enqueue next message (response), if any */
	conn->rmsg = nmsg;

	if (dnode_rsp_filter(ctx, conn, msg)) {
		return;
	}

	dnode_rsp_forward(ctx, conn, msg);
}

struct msg *
dnode_rsp_send_next(struct context *ctx, struct conn *conn)
{
	loga("dnode_rsp_send_next 333333333333333333333333333333333333333333333333");
	ASSERT(conn->dnode_client && !conn->dnode_server);
	struct msg *msg = rsp_send_next(ctx, conn);

	if (msg != NULL && conn->dyn_mode) {
		loga("dnode_rsp_send_next 444444444444444444444444444444444444444444");
		struct msg *pmsg = TAILQ_FIRST(&conn->omsg_q); //peer request's msg

		//add dnode header
		struct mbuf *nbuf = mbuf_get();
		if (nbuf == NULL) {
			return NULL; //need to address error here properly
		}

		//dyn message's meta data
		//uint64_t msg_id = pmsg->dmsg->id;
		uint64_t msg_id = 0;

		dmsg_write(nbuf, msg_id, DMSG_RES, conn);
		mbuf_insert_head(&msg->mhdr, nbuf);

		log_hexdump(LOG_VERB, nbuf->pos, mbuf_length(nbuf), "dyn message 222 header: ");
		struct mbuf *b = STAILQ_LAST(&msg->mhdr, mbuf, next);
		log_hexdump(LOG_VERB, b->pos, mbuf_length(b), "dyn message 222 payload: ");
	}
	return msg;
}

void
dnode_rsp_send_done(struct context *ctx, struct conn *conn, struct msg *msg)
{
	loga("dnode_rsp_send_done 2222222222222222222222222222222222222222222222");
	struct msg *pmsg; /* peer message (request) */

	ASSERT(conn->dnode_client && !conn->dnode_server);
	ASSERT(conn->smsg == NULL);

	log_debug(LOG_VVERB, "dyn: send done rsp %"PRIu64" on c %d", msg->id, conn->sd);

	pmsg = msg->peer;

	ASSERT(!msg->request && pmsg->request);
	ASSERT(pmsg->peer == msg);
	ASSERT(pmsg->done && !pmsg->swallow);

	/* dequeue request from client outq */
	conn->dequeue_outq(ctx, conn, pmsg);

	req_put(pmsg);
}


