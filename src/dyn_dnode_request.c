/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 

#include "dyn_core.h"
#include "dyn_server.h"
#include "dyn_dnode_peer.h"

static struct string client_request_dyn_msg = string("Client_request");
static uint64_t peer_msg_id = 0;

static uint8_t version = VERSION_10;

struct msg *
dnode_req_get(struct conn *conn)
{
	struct msg *msg;

	ASSERT(conn->dnode_client && !conn->dnode_server);

	msg = msg_get(conn, true, conn->redis);
	if (msg == NULL) {
		conn->err = errno;
	}

	return msg;
}

void
dnode_req_put(struct msg *msg)
{
	req_put(msg);
}


bool
dnode_req_done(struct conn *conn, struct msg *msg)
{
	ASSERT(conn->dnode_client && !conn->dnode_server);
	return req_done(conn, msg);
}


bool
dnode_req_error(struct conn *conn, struct msg *msg)
{
	ASSERT(msg->request && dnode_req_done(conn, msg));
	return req_error(conn, msg);
}

void
dnode_req_peer_enqueue_imsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
	ASSERT(msg->request);
	ASSERT(!conn->dnode_client && !conn->dnode_server);

	req_server_enqueue_imsgq(ctx, conn, msg);
}

void
dnode_req_peer_dequeue_imsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
	ASSERT(msg->request);
	ASSERT(!conn->dnode_client && !conn->dnode_server);

	TAILQ_REMOVE(&conn->imsg_q, msg, s_tqe);

	//fixme for peer stats
	/* stats_server_decr(ctx, conn->owner, in_queue); */
	/* stats_server_decr_by(ctx, conn->owner, in_queue_bytes, msg->mlen); */
	struct server_pool *pool = (struct server_pool *) array_get(&ctx->pool, 0);
    stats_pool_decr(ctx, pool, peer_in_queue);
    stats_pool_decr_by(ctx, pool, peer_in_queue_bytes, msg->mlen);
}

void
dnode_req_client_enqueue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
	ASSERT(msg->request);
	ASSERT(conn->dnode_client && !conn->dnode_server);

	TAILQ_INSERT_TAIL(&conn->omsg_q, msg, c_tqe);
}

void
dnode_req_peer_enqueue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
	ASSERT(msg->request);
	ASSERT(!conn->dnode_client && !conn->dnode_server);

	TAILQ_INSERT_TAIL(&conn->omsg_q, msg, s_tqe);

	//fixme for peer stats
	/* stats_server_incr(ctx, conn->owner, out_queue); */
	/* stats_server_incr_by(ctx, conn->owner, out_queue_bytes, msg->mlen); */

	//use only the 1st pool
	struct server_pool *pool = (struct server_pool *) array_get(&ctx->pool, 0);
	stats_pool_incr(ctx, pool, peer_out_queue);
	stats_pool_incr_by(ctx, pool, peer_out_queue_bytes, msg->mlen);
}

void
dnode_req_client_dequeue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
	ASSERT(msg->request);
	ASSERT(conn->dnode_client && !conn->dnode_server);

	TAILQ_REMOVE(&conn->omsg_q, msg, c_tqe);
}

void
dnode_req_peer_dequeue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
	ASSERT(msg->request);
	ASSERT(!conn->dnode_client && !conn->dnode_server);

	msg_tmo_delete(msg);

	TAILQ_REMOVE(&conn->omsg_q, msg, s_tqe);

	//fixme for peer stats
	/* stats_server_decr(ctx, conn->owner, out_queue); */
	/* stats_server_decr_by(ctx, conn->owner, out_queue_bytes, msg->mlen); */

	//use the 1st pool
	struct server_pool *pool = (struct server_pool *) array_get(&ctx->pool, 0);
	stats_pool_decr(ctx, pool, peer_out_queue);
    stats_pool_decr_by(ctx, pool, peer_out_queue_bytes, msg->mlen);
}

struct msg *
dnode_req_recv_next(struct context *ctx, struct conn *conn, bool alloc)
{
	struct msg *msg;

	ASSERT(conn->dnode_client && !conn->dnode_server);
	return req_recv_next(ctx, conn, alloc);
}

static bool
dnode_req_filter(struct context *ctx, struct conn *conn, struct msg *msg)
{
	ASSERT(conn->dnode_client && !conn->dnode_server);

	if (msg_empty(msg)) {
		ASSERT(conn->rmsg == NULL);
		log_debug(LOG_VERB, "dyn: filter empty req %"PRIu64" from c %d", msg->id,
				conn->sd);
		dnode_req_put(msg);
		return true;
	}


	/* dynomite hanlder */
	if (msg->dmsg != NULL) {
		return dmsg_process(ctx, conn, msg->dmsg);
	}


	return false;
}

static void
dnode_req_forward_error(struct context *ctx, struct conn *conn, struct msg *msg)
{
	rstatus_t status;

	ASSERT(conn->dnode_client && !conn->dnode_server);

	log_debug(LOG_INFO, "dyn: forward req %"PRIu64" len %"PRIu32" type %d from "
			"c %d failed: %s", msg->id, msg->mlen, msg->type, conn->sd,
			strerror(errno));

	msg->done = 1;
	msg->error = 1;
	msg->err = errno;

	/* noreply request don't expect any response */
	if (msg->noreply) {
		dnode_req_put(msg);
		return;
	}

	if (dnode_req_done(conn, TAILQ_FIRST(&conn->omsg_q))) {
		status = event_add_out(ctx->evb, conn);
		if (status != DN_OK) {
			conn->err = errno;
		}
	}
}


static void
dnode_req_forward(struct context *ctx, struct conn *conn, struct msg *msg)
{
	struct server_pool *pool;
	uint8_t *key;
	uint32_t keylen;

	ASSERT(conn->dnode_client && !conn->dnode_server);

	pool = conn->owner;
	key = NULL;
	keylen = 0;

	if (!string_empty(&pool->hash_tag)) {
		struct string *tag = &pool->hash_tag;
		uint8_t *tag_start, *tag_end;

		tag_start = dn_strchr(msg->key_start, msg->key_end, tag->data[0]);
		if (tag_start != NULL) {
			tag_end = dn_strchr(tag_start + 1, msg->key_end, tag->data[1]);
			if (tag_end != NULL) {
				key = tag_start + 1;
				keylen = (uint32_t)(tag_end - key);
			}
		}
	}

	if (keylen == 0) {
		key = msg->key_start;
		keylen = (uint32_t)(msg->key_end - msg->key_start);
	}

	local_req_forward(ctx, conn, msg, key, keylen);
}


void
dnode_req_recv_done(struct context *ctx, struct conn *conn, struct msg *msg,
		struct msg *nmsg)
{
	ASSERT(conn->dnode_client && !conn->dnode_server);
	ASSERT(msg->request);
	ASSERT(msg->owner == conn);
	ASSERT(conn->rmsg == msg);
	ASSERT(nmsg == NULL || nmsg->request);

	/* enqueue next message (request), if any */
	conn->rmsg = nmsg;

	if (dnode_req_filter(ctx, conn, msg)) {
		return;
	}

	dnode_req_forward(ctx, conn, msg);
}

struct msg *
dnode_req_send_next(struct context *ctx, struct conn *conn)
{
	rstatus_t status;
	struct msg *msg, *nmsg; /* current and next message */

	ASSERT(!conn->dnode_client && !conn->dnode_server);

	return req_send_next(ctx, conn);
}

void
dnode_req_send_done(struct context *ctx, struct conn *conn, struct msg *msg)
{
	ASSERT(!conn->dnode_client && !conn->dnode_server);
	req_send_done(ctx, conn, msg);
}


static void
dnode_req_forward_stats(struct context *ctx, struct server *server, struct msg *msg)
{
    ASSERT(msg->request);
    //use only the 1st pool
    struct server_pool *pool = (struct server_pool *) array_get(&ctx->pool, 0);
	stats_pool_incr(ctx, pool, peer_requests);
	stats_pool_incr_by(ctx, pool, peer_request_bytes, msg->mlen);
}

void
peer_req_forward(struct context *ctx, struct conn *c_conn, struct conn *p_conn, struct msg *msg,
		struct datacenter *dc, uint8_t *key, uint32_t keylen)
{

	rstatus_t status;
	/* enqueue message (request) into client outq, if response is expected */
	if (!msg->noreply) {
		c_conn->enqueue_outq(ctx, c_conn, msg);
	}

	ASSERT(!p_conn->dnode_client && !p_conn->dnode_server);
	ASSERT(c_conn->client);

	/* enqueue the message (request) into peer inq */
	if (TAILQ_EMPTY(&p_conn->imsg_q)) {
		status = event_add_out(ctx->evb, p_conn);
		if (status != DN_OK) {
			dnode_req_forward_error(ctx, p_conn, msg);
			p_conn->err = errno;
			return;
		}
	}

	struct mbuf *nbuf = mbuf_get();
	if (nbuf == NULL) {
		return;
	}

	//dyn message's meta data
	uint64_t msg_id = peer_msg_id++;

	dmsg_write(nbuf, msg_id, DMSG_REQ, version, &client_request_dyn_msg);
	mbuf_insert_head(&msg->mhdr, nbuf);

	p_conn->enqueue_inq(ctx, p_conn, msg);

	//fix me - coordinator stats
	//req_forward_stats(ctx, s_conn->owner, msg);
	dnode_req_forward_stats(ctx, p_conn->owner, msg);

	log_debug(LOG_VERB, "remote forward from c %d to s %d req %"PRIu64" len %"PRIu32
			" type %d with key '%.*s'", c_conn->sd, p_conn->sd, msg->id,
			msg->mlen, msg->type, keylen, key);
}


/*
 //for sending a string over to a peer
void
peer_gossip_forward1(struct context *ctx, struct conn *conn, bool redis, struct string *data)
{
	rstatus_t status;
	struct msg *msg = msg_get(conn, 1, redis);

	if (msg == NULL) {
		log_debug(LOG_DEBUG, "Unable to obtain a msg");
		return;
	}

	struct mbuf *nbuf = mbuf_get();
	if (nbuf == NULL) {
        log_debug(LOG_DEBUG, "Unable to obtain a mbuf");
        msg_put(msg);
        return;
	}

    uint64_t msg_id = peer_msg_id++;

	dmsg_write(nbuf, msg_id, GOSSIP_SYN, version, data);
	mbuf_insert_head(&msg->mhdr, nbuf);

    if (TAILQ_EMPTY(&conn->imsg_q)) {
        status = event_add_out(ctx->evb, conn);
        if (status != DN_OK) {
            dnode_req_forward_error(ctx, conn, msg);
            conn->err = errno;
            return;
        }
    }

	conn->enqueue_inq(ctx, conn, msg);


    log_debug(LOG_VERB, "gossip to peer %d with msg_id %"PRIu64" '%.*s'", conn->sd, msg->id,
    		             data->len, data->data);

}
 */

/*
 * Sending a mbuf of gossip data over the wire for a peer
 */
void
peer_gossip_forward(struct context *ctx, struct conn *conn, bool redis, struct mbuf *mbuf)
{
	rstatus_t status;
	struct msg *msg = msg_get(conn, 1, redis);

	if (msg == NULL) {
		log_debug(LOG_DEBUG, "Unable to obtain a msg");
		return;
	}

	struct mbuf *nbuf = mbuf_get();
	if (nbuf == NULL) {
		log_debug(LOG_DEBUG, "Unable to obtain a mbuf");
		msg_put(msg);
		return;
	}

	uint64_t msg_id = peer_msg_id++;

	dmsg_write_mbuf(nbuf, msg_id, GOSSIP_SYN, version, mbuf);
	mbuf_insert_head(&msg->mhdr, nbuf);

	mbuf_put(mbuf); //free this as nobody else will do

	/* enqueue the message (request) into peer inq */
	if (TAILQ_EMPTY(&conn->imsg_q)) {
		status = event_add_out(ctx->evb, conn);
		if (status != DN_OK) {
			dnode_req_forward_error(ctx, conn, msg);
			conn->err = errno;
			return;
		}
	}

	//need to handle a reply
	//conn->enqueue_outq(ctx, conn, msg);

	//msg->noreply = 1;
	conn->enqueue_inq(ctx, conn, msg);

	//fix me - gossip stats
	//req_forward_stats(ctx, s_conn->owner, msg);

	//log_debug(LOG_VERB, "gossip to peer %d with msg_id %"PRIu64" '%.*s'", conn->sd, msg->id,
	//		             data->len, data->data);
}
