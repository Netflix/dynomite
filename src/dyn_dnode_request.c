/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 

#include "dyn_core.h"
#include "dyn_dnode_peer.h"
#include "dyn_mbuf.h"
#include "dyn_server.h"


//static struct string client_request_dyn_msg = string("Client_request");
static uint64_t peer_msg_id = 0;


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
	ASSERT(!conn->dnode_client && !conn->dnode_server);
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
		if (dmsg_process(ctx, conn, msg->dmsg)) {
			dnode_req_put(msg);
			return true;
		}
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
dnode_req_local_forward(struct context *ctx, struct conn *conn, struct msg *msg)
{
	struct server_pool *pool;
	uint8_t *key;
	uint32_t keylen;
	log_debug(LOG_VERB, "dnode_req_local_forward entering ");

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
dnode_req_recv_done(struct context *ctx, struct conn *conn,
		            struct msg *msg, struct msg *nmsg)
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

	dnode_req_local_forward(ctx, conn, msg);
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
	log_debug(LOG_VERB, "dnode_req_send_done entering!!!");
	ASSERT(!conn->dnode_client && !conn->dnode_server);
	req_send_done(ctx, conn, msg);
}


static void
dnode_peer_req_forward_stats(struct context *ctx, struct server *server, struct msg *msg)
{
        ASSERT(msg->request);
        //use only the 1st pool
        //struct server_pool *pool = (struct server_pool *) array_get(&ctx->pool, 0);
        struct server_pool *pool = server->owner;
        stats_pool_incr(ctx, pool, peer_requests);
        stats_pool_incr_by(ctx, pool, peer_request_bytes, msg->mlen);
}


/* Forward a client request over to a peer */
void
dnode_peer_req_forward(struct context *ctx, struct conn *c_conn, struct conn *p_conn,
		struct msg *msg, struct rack *rack, uint8_t *key, uint32_t keylen)
{

    if (get_tracking_level() >= LOG_VVERB) {
	   log_debug(LOG_NOTICE, "dnode_peer_req_forward entering");
    }

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

	uint64_t msg_id = peer_msg_id++;

	struct mbuf *header_buf = mbuf_get();
	if (header_buf == NULL) {
		loga("Unable to obtain an mbuf for dnode msg's header!");
		return;
	}

	if (p_conn->dnode_secured) {
		//Encrypting and adding header for a request
		struct mbuf *data_buf = STAILQ_LAST(&msg->mhdr, mbuf, next);

		//TODOs: need to deal with multi-block later
		log_debug(LOG_VERB, "AES encryption key: %s\n", base64_encode(p_conn->aes_key, AES_KEYLEN));

		//write dnode header
		if (ENCRYPTION) {
			struct mbuf *encrypted_buf = mbuf_get();
			if (encrypted_buf == NULL) {
				loga("Unable to obtain an mbuf for encryption!");
				return; //TODOs: need to clean up
			}

			status = dyn_aes_encrypt(data_buf->pos, mbuf_length(data_buf), encrypted_buf, p_conn->aes_key);
			log_debug(LOG_VERB, "#encrypted bytes : %d", status);

			dmsg_write(header_buf, msg_id, DMSG_REQ, p_conn, mbuf_length(encrypted_buf));

		    log_hexdump(LOG_VERB, data_buf->pos, mbuf_length(data_buf), "dyn message original payload: ");
			log_hexdump(LOG_VERB, encrypted_buf->pos, mbuf_length(encrypted_buf), "dyn message encrypted payload: ");

			//remove the original dbuf out of the queue and insert encrypted mbuf to replace
			mbuf_remove(&msg->mhdr, data_buf);
			mbuf_insert(&msg->mhdr, encrypted_buf);
			//free it as no one will need it again
			mbuf_put(data_buf);
		} else {
			log_debug(LOG_VERB, "no encryption on the msg payload");
			dmsg_write(header_buf, msg_id, DMSG_REQ, p_conn, mbuf_length(data_buf));
		}

	} else {
		//write dnode header
		dmsg_write(header_buf, msg_id, DMSG_REQ, p_conn, 0);
	}

	mbuf_insert_head(&msg->mhdr, header_buf);


    if (get_tracking_level() >= LOG_VVERB) {
	   log_hexdump(LOG_NOTICE, header_buf->pos, mbuf_length(header_buf), "dyn message header: ");
	   msg_dump(msg);
    }

	p_conn->enqueue_inq(ctx, p_conn, msg);

	dnode_peer_req_forward_stats(ctx, p_conn->owner, msg);


    if (get_tracking_level() >= LOG_VERB) {
	   log_debug(LOG_NOTICE, "remote forward from c %d to s %d req %"PRIu64" len %"PRIu32
		   	     " type %d with key '%.*s'", c_conn->sd, p_conn->sd, msg->id,
			     msg->mlen, msg->type, keylen, key);
    }

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
 * Sending a mbuf of gossip data over the wire to a peer
 */
void
dnode_peer_gossip_forward(struct context *ctx, struct conn *conn, bool redis, struct mbuf *data_buf)
{
	rstatus_t status;
	struct msg *msg = msg_get(conn, 1, redis);

	if (msg == NULL) {
		log_debug(LOG_DEBUG, "Unable to obtain a msg");
		return;
	}

	struct mbuf *header_buf = mbuf_get();
	if (header_buf == NULL) {
		log_debug(LOG_DEBUG, "Unable to obtain a data_buf");
		msg_put(msg);
		return;
	}

	uint64_t msg_id = peer_msg_id++;

	if (conn->dnode_secured) {
		log_debug(LOG_VERB, "Assemble a secured msg to send");
		log_debug(LOG_VERB, "AES encryption key: %s\n", base64_encode(conn->aes_key, AES_KEYLEN));

		if (ENCRYPTION) {
		   struct mbuf *encrypted_buf = mbuf_get();
		   if (encrypted_buf == NULL) {
			  loga("Unable to obtain an data_buf for encryption!");
			  return; //TODOs: need to clean up
		   }

		   status = dyn_aes_encrypt(data_buf->pos, mbuf_length(data_buf), encrypted_buf, conn->aes_key);
		   log_debug(LOG_VERB, "#encrypted bytes : %d", status);

		   //write dnode header
		   dmsg_write(header_buf, msg_id, GOSSIP_SYN, conn, mbuf_length(encrypted_buf));


	       if (get_tracking_level() >= LOG_VVERB) {
		      log_hexdump(LOG_NOTICE, data_buf->pos, mbuf_length(data_buf), "dyn message original payload: ");
		      log_hexdump(LOG_NOTICE, encrypted_buf->pos, mbuf_length(encrypted_buf), "dyn message encrypted payload: ");
           }

	       mbuf_remove(&msg->mhdr, data_buf);
		   mbuf_insert(&msg->mhdr, encrypted_buf);
		   //free data_buf as no one will need it again
		   mbuf_put(data_buf);  //TODOS: need to remove this from the msg->mhdr as in the other method

		} else {
		   log_debug(LOG_VVERB, "No encryption");
		   dmsg_write_mbuf(header_buf, msg_id, GOSSIP_SYN, conn, mbuf_length(data_buf));
		   mbuf_insert(&msg->mhdr, data_buf);
		}

	} else {
       log_debug(LOG_VVERB, "Assemble a non-secured msg to send");
	   dmsg_write_mbuf(header_buf, msg_id, GOSSIP_SYN, conn, mbuf_length(data_buf));
	   mbuf_insert(&msg->mhdr, data_buf);
	}

	mbuf_insert_head(&msg->mhdr, header_buf);

    if (get_tracking_level() >= LOG_VVERB) {
	   log_hexdump(LOG_NOTICE, header_buf->pos, mbuf_length(header_buf), "dyn gossip message header: ");
	   msg_dump(msg);
    }

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

	msg->noreply = 1;
	conn->enqueue_inq(ctx, conn, msg);

}
