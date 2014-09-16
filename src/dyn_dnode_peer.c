/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 

#include <stdlib.h>
#include <unistd.h>

#include "dyn_core.h"
#include "dyn_conf.h"
#include "dyn_server.h"
#include "dyn_dnode_peer.h"
#include "hashkit/dyn_token.h"


void
dnode_peer_ref(struct conn *conn, void *owner)
{
	struct server *pn = owner;

	ASSERT(!conn->dnode_server && !conn->dnode_client);
	ASSERT(conn->owner == NULL);

	conn->family = pn->family;
	conn->addrlen = pn->addrlen;
	conn->addr = pn->addr;

	pn->ns_conn_q++;
	TAILQ_INSERT_TAIL(&pn->s_conn_q, conn, conn_tqe);

	conn->owner = owner;

	log_debug(LOG_VVERB, "dyn: ref peer conn %p owner %p into '%.*s", conn, pn,
			pn->pname.len, pn->pname.data);
}

void
dnode_peer_unref(struct conn *conn)
{
	struct server *pn;

	ASSERT(!conn->dnode_server && !conn->dnode_client);
	ASSERT(conn->owner != NULL);

	pn = conn->owner;
	conn->owner = NULL;

	ASSERT(pn->ns_conn_q != 0);
	pn->ns_conn_q--;
	TAILQ_REMOVE(&pn->s_conn_q, conn, conn_tqe);

	log_debug(LOG_VVERB, "dyn: unref peer conn %p owner %p from '%.*s'", conn, pn,
			pn->pname.len, pn->pname.data);
}

int
dnode_peer_timeout(struct conn *conn)
{
	struct server *server;
	struct server_pool *pool;

	ASSERT(!conn->dnode_server && !conn->dnode_client);

	server = conn->owner;
	pool = server->owner;

	return pool->d_timeout;
}

bool
dnode_peer_active(struct conn *conn)
{
	ASSERT(!conn->dnode_server && !conn->dnode_client);

	if (!TAILQ_EMPTY(&conn->imsg_q)) {
		log_debug(LOG_VVERB, "dyn: s %d is active", conn->sd);
		return true;
	}

	if (!TAILQ_EMPTY(&conn->omsg_q)) {
		log_debug(LOG_VVERB, "dyn: s %d is active", conn->sd);
		return true;
	}

	if (conn->rmsg != NULL) {
		log_debug(LOG_VVERB, "dyn: s %d is active", conn->sd);
		return true;
	}

	if (conn->smsg != NULL) {
		log_debug(LOG_VVERB, "dyn: s %d is active", conn->sd);
		return true;
	}

	log_debug(LOG_VVERB, "dyn: s %d is inactive", conn->sd);

	return false;
}

static rstatus_t
dnode_peer_each_set_owner(void *elem, void *data)
{
	struct server *s = elem;
	struct server_pool *sp = data;

	s->owner = sp;

	return DN_OK;
}

static rstatus_t
dnode_peer_add_local(struct server_pool *pool, struct server *peer)
{
	ASSERT(peer != NULL);
	peer->idx = 0; /* this might be psychotic, trying it for now */

	peer->pname = pool->d_addrstr;

	uint8_t *p = pool->d_addrstr.data + pool->d_addrstr.len - 1;
	uint8_t *start = pool->d_addrstr.data;
	string_copy(&peer->name, start, dn_strrchr(p, start, ':') - start);

	//peer->name = pool->d_addrstr;
	peer->port = pool->d_port;

	peer->weight = 0;  /* hacking this out of the way for now */
	peer->dc = pool->dc;
	peer->is_local = true;
	//TODO-jeb might need to copy over tokens, not sure if this is good enough
	peer->tokens = pool->tokens;

	peer->family = pool->d_family;
	peer->addrlen = pool->d_addrlen;
	peer->addr = pool->d_addr;

	peer->ns_conn_q = 0;
	TAILQ_INIT(&peer->s_conn_q);

	peer->next_retry = 0LL;
	peer->failure_count = 0;
	peer->is_seed = 1;
	string_copy(&peer->region, pool->region.data, pool->region.len);
	peer->owner = pool;

	log_debug(LOG_VERB, "dyn: transform to local node to peer %"PRIu32" '%.*s'",
			peer->idx, pool->name.len, pool->name.data);

	return DN_OK;
}

rstatus_t
dnode_peer_init(struct array *server_pool, struct context *ctx)
{
	rstatus_t status;

	status = array_each(server_pool, dnode_peer_each_pool_init, ctx);
	if (status != DN_OK) {
		server_pool_deinit(server_pool);
		return status;
	}

	return DN_OK;
}


rstatus_t
dnode_peer_each_pool_init(void *elem, void *context)
{
	struct server_pool *sp = (struct server_pool *) elem;
	struct context *ctx = context;
	struct array *conf_seeds = &sp->conf_pool->dyn_seeds;

	struct array * seeds = &sp->seeds;
	struct array * peers = &sp->peers;
	rstatus_t status;
	uint32_t nseed;

	/* init seeds list */
	nseed = array_n(conf_seeds);
	if(nseed == 0) {
		log_debug(LOG_INFO, "dyn: look like you are running with no seeds deifined. This is ok for running with just one node.");

		// add current node to peers array
		status = array_init(peers, CONF_DEFAULT_PEERS, sizeof(struct server));
		if (status != DN_OK) {
			return status;
		}

		struct server *peer = array_push(peers);
		ASSERT(peer != NULL);
		status = dnode_peer_add_local(sp, peer);
		if (status != DN_OK) {
			dnode_peer_deinit(peers);
		}
		dnode_peer_pool_run(sp);
		return status;
	}

	ASSERT(array_n(seeds) == 0);

	status = array_init(seeds, nseed, sizeof(struct server));
	if (status != DN_OK) {
		return status;
	}

	/* transform conf seeds to seeds */
	status = array_each(conf_seeds, conf_seed_each_transform, seeds);
	if (status != DN_OK) {
		dnode_peer_deinit(seeds);
		return status;
	}
	ASSERT(array_n(seeds) == nseed);

	/* set seed owner */
	status = array_each(seeds, dnode_peer_each_set_owner, sp);
	if (status != DN_OK) {
		dnode_peer_deinit(seeds);
		return status;
	}


	/* initialize peers list = seeds list */
	ASSERT(array_n(peers) == 0);

	// add current node to peers array
	uint32_t peer_cnt = nseed + 1;
	status = array_init(peers, CONF_DEFAULT_PEERS, sizeof(struct server));
	if (status != DN_OK) {
		return status;
	}

	struct server *peer = array_push(peers);
	ASSERT(peer != NULL);
	status = dnode_peer_add_local(sp, peer);
	if (status != DN_OK) {
		dnode_peer_deinit(seeds);
		dnode_peer_deinit(peers);
		return status;
	}

	status = array_each(conf_seeds, conf_seed_each_transform, peers);
	if (status != DN_OK) {
		dnode_peer_deinit(seeds);
		dnode_peer_deinit(peers);
		return status;
	}
	ASSERT(array_n(peers) == peer_cnt);

	status = array_each(peers, dnode_peer_each_set_owner, sp);
	if (status != DN_OK) {
		dnode_peer_deinit(seeds);
		dnode_peer_deinit(peers);
		return status;
	}

	dnode_peer_pool_run(sp);

	log_debug(LOG_DEBUG, "init %"PRIu32" seeds and peers in pool %"PRIu32" '%.*s'",
			nseed, sp->idx, sp->name.len, sp->name.data);

	return DN_OK;
}

void
dnode_peer_deinit(struct array *nodes)
{
	uint32_t i, nnode;

	for (i = 0, nnode = array_n(nodes); i < nnode; i++) {
		struct server *s;

		s = array_pop(nodes);
		ASSERT(TAILQ_EMPTY(&s->s_conn_q) && s->ns_conn_q == 0);
	}
	array_deinit(nodes);
}


struct conn *
dnode_peer_conn(struct server *server)
{
	struct server_pool *pool;
	struct conn *conn;

	pool = server->owner;

	if (server->ns_conn_q < pool->d_connections) {
		return conn_get_peer(server, false, pool->redis);
	}
	ASSERT(server->ns_conn_q == pool->d_connections);

	/*
	 * Pick a server connection from the head of the queue and insert
	 * it back into the tail of queue to maintain the lru order
	 */
	conn = TAILQ_FIRST(&server->s_conn_q);
	ASSERT(!conn->dnode_client && !conn->dnode_server);

	TAILQ_REMOVE(&server->s_conn_q, conn, conn_tqe);
	TAILQ_INSERT_TAIL(&server->s_conn_q, conn, conn_tqe);

	return conn;
}


static rstatus_t
dnode_peer_each_preconnect(void *elem, void *data)
{
	rstatus_t status;
	struct server *peer;
	struct server_pool *sp;
	struct conn *conn;

	peer = elem;
	sp = peer->owner;

	conn = dnode_peer_conn(peer);
	if (conn == NULL) {
		return DN_ENOMEM;
	}

	status = dnode_peer_connect(sp->ctx, peer, conn);
	if (status != DN_OK) {
		log_warn("dyn: connect to peer '%.*s' failed, ignored: %s",
				peer->pname.len, peer->pname.data, strerror(errno));
		dnode_peer_close(sp->ctx, conn);
	}

	return DN_OK;
}

static rstatus_t
dnode_peer_each_disconnect(void *elem, void *data)
{
	struct server *server;
	struct server_pool *pool;

	server = elem;
	pool = server->owner;

	//TODOs: fixe me not to use s_conn_q to distinguish server pool and peer pool
	while (!TAILQ_EMPTY(&server->s_conn_q)) {
		struct conn *conn;

		ASSERT(server->ns_conn_q > 0);

		conn = TAILQ_FIRST(&server->s_conn_q);
		conn->close(pool->ctx, conn);
	}

	return DN_OK;
}

static void
dnode_peer_failure(struct context *ctx, struct server *server)
{
	struct server_pool *pool = server->owner;
	int64_t now, next;
	rstatus_t status;

	//fix me
	if (!pool->auto_eject_hosts) {
		return;
	}

	server->failure_count++;

	log_debug(LOG_VERB, "dyn: peer '%.*s' failure count %"PRIu32" limit %"PRIu32,
			server->pname.len, server->pname.data, server->failure_count,
			pool->server_failure_limit);

	if (server->failure_count < pool->server_failure_limit) {
		return;
	}

	now = dn_usec_now();
	if (now < 0) {
		return;
	}

	//fix me
	//stats_server_set_ts(ctx, server, server_ejected_at, now);

	//fix me
	next = now + pool->server_retry_timeout;

	log_debug(LOG_INFO, "dyn: update peer pool %"PRIu32" '%.*s' to delete peer '%.*s' "
			"for next %"PRIu32" secs", pool->idx, pool->name.len,
			pool->name.data, server->pname.len, server->pname.data,
			pool->server_retry_timeout / 1000 / 1000);

	//stats_pool_incr(ctx, pool, server_ejects);

	server->failure_count = 0;
	server->next_retry = next;

	status = dnode_peer_pool_run(pool);
	if (status != DN_OK) {
		log_error("dyn: updating peer pool %"PRIu32" '%.*s' failed: %s", pool->idx,
				pool->name.len, pool->name.data, strerror(errno));
	}
}

static void
dnode_peer_close_stats(struct context *ctx, struct server *server, err_t err,
		unsigned eof, unsigned connected)
{
	if (connected) {
		//fix me
		//stats_server_decr(ctx, server, server_connections);
	}

	if (eof) {
		//fix me
		//stats_server_incr(ctx, server, server_eof);
		return;
	}

	switch (err) {
	case ETIMEDOUT:
		//stats_server_incr(ctx, server, server_timedout);
		break;
	case EPIPE:
	case ECONNRESET:
	case ECONNABORTED:
	case ECONNREFUSED:
	case ENOTCONN:
	case ENETDOWN:
	case ENETUNREACH:
	case EHOSTDOWN:
	case EHOSTUNREACH:
	default:
		//stats_server_incr(ctx, server, server_err);
		break;
	}
}

void
dnode_peer_close(struct context *ctx, struct conn *conn)
{
	rstatus_t status;
	struct msg *msg, *nmsg; /* current and next message */
	struct conn *c_conn;    /* peer client connection */

	ASSERT(!conn->dnode_server && !conn->dnode_client);

	dnode_peer_close_stats(ctx, conn->owner, conn->err, conn->eof,
			conn->connected);

	if (conn->sd < 0) {
		dnode_peer_failure(ctx, conn->owner);
		conn->unref(conn);
		conn_put(conn);
		return;
	}

	for (msg = TAILQ_FIRST(&conn->imsg_q); msg != NULL; msg = nmsg) {
		nmsg = TAILQ_NEXT(msg, s_tqe);

		/* dequeue the message (request) from server inq */
		conn->dequeue_inq(ctx, conn, msg);

		/*
		 * Don't send any error response, if
		 * 1. request is tagged as noreply or,
		 * 2. client has already closed its connection
		 */
		if (msg->swallow || msg->noreply) {
			log_debug(LOG_INFO, "dyn: close s %d swallow req %"PRIu64" len %"PRIu32
					  " type %d", conn->sd, msg->id, msg->mlen, msg->type);
			req_put(msg);
		} else {
			c_conn = msg->owner;
			ASSERT(c_conn->dnode_client && !c_conn->dnode_server);

			msg->done = 1;
			msg->error = 1;
			msg->err = conn->err;
			msg->dyn_error = PEER_CONNECTION_REFUSE;

			if (TAILQ_FIRST(&c_conn->omsg_q) != NULL && req_done(c_conn, TAILQ_FIRST(&c_conn->omsg_q))) {
				event_add_out(ctx->evb, msg->owner);
			}

			log_debug(LOG_INFO, "dyn: close s %d schedule error for req %"PRIu64" "
					  "len %"PRIu32" type %d from c %d%c %s", conn->sd, msg->id,
					  msg->mlen, msg->type, c_conn->sd, conn->err ? ':' : ' ',
					  conn->err ? strerror(conn->err): " ");
			}
	}
	ASSERT(TAILQ_EMPTY(&conn->imsg_q));

	for (msg = TAILQ_FIRST(&conn->omsg_q); msg != NULL; msg = nmsg) {
		nmsg = TAILQ_NEXT(msg, s_tqe);

		/* dequeue the message (request) from server outq */
		conn->dequeue_outq(ctx, conn, msg);

		if (msg->swallow) {
			log_debug(LOG_INFO, "dyn: close s %d swallow req %"PRIu64" len %"PRIu32
					" type %d", conn->sd, msg->id, msg->mlen, msg->type);
			req_put(msg);
		} else {
			c_conn = msg->owner;
			ASSERT(c_conn->dnode_client && !c_conn->dnode_server);

			msg->done = 1;
			msg->error = 1;
			msg->err = conn->err;

			if (TAILQ_FIRST(&c_conn->omsg_q) != NULL && req_done(c_conn, TAILQ_FIRST(&c_conn->omsg_q))) {
				event_add_out(ctx->evb, msg->owner);
			}

			log_debug(LOG_INFO, "dyn: close s %d schedule error for req %"PRIu64" "
					"len %"PRIu32" type %d from c %d%c %s", conn->sd, msg->id,
					msg->mlen, msg->type, c_conn->sd, conn->err ? ':' : ' ',
							conn->err ? strerror(conn->err): " ");
		}
	}
	ASSERT(TAILQ_EMPTY(&conn->omsg_q));

	msg = conn->rmsg;
	if (msg != NULL) {
		conn->rmsg = NULL;

		ASSERT(!msg->request);
		ASSERT(msg->peer == NULL);

		rsp_put(msg);

		log_debug(LOG_INFO, "dyn: close s %d discarding rsp %"PRIu64" len %"PRIu32" "
				"in error", conn->sd, msg->id, msg->mlen);
	}

	ASSERT(conn->smsg == NULL);

	dnode_peer_failure(ctx, conn->owner);

	conn->unref(conn);

	status = close(conn->sd);
	if (status < 0) {
		log_error("dyn: close s %d failed, ignored: %s", conn->sd, strerror(errno));
	}
	conn->sd = -1;

	conn_put(conn);
}


rstatus_t
dnode_peer_handshake_announcing(struct server_pool *sp)
{
	rstatus_t status;
	log_debug(LOG_VVERB, "dyn: handshaking peers");
	struct array *peers = &sp->peers;

	uint32_t i,nelem;
	nelem = array_n(peers);

	//we assume one mbuf is enough for now - will enhance with multiple mbufs later
    struct mbuf *mbuf = mbuf_get();
    if (mbuf == NULL) {
       log_debug(LOG_VVERB, "Too bad, not enough memory!");
       return DN_ENOMEM;
    }

    //annoucing myself by sending msg: 'region-dc-token,started_ts,apps_version,node_state,node_dns'
    mbuf_write_string(mbuf, &sp->region);
    mbuf_write_char(mbuf, '-');
    mbuf_write_string(mbuf, &sp->dc);
    mbuf_write_char(mbuf, '-');
    struct dyn_token *token = (struct dyn_token *) array_get(&sp->tokens, 0);
    if (token == NULL) {
       log_debug(LOG_VVERB, "Why? This should not be null!");
       mbuf_put(mbuf);
       return DN_ERROR;
    }

    mbuf_write_uint32(mbuf, token->mag[0]);
    mbuf_write_char(mbuf, ',');
    int64_t cur_ts = (int64_t)time(NULL);
    mbuf_write_uint64(mbuf, cur_ts);
    mbuf_write_char(mbuf, ',');
    mbuf_write_uint8(mbuf, VERSION_10);
    mbuf_write_char(mbuf, ',');
    mbuf_write_uint8(mbuf, sp->ctx->dyn_state);
    mbuf_write_char(mbuf, ',');
    struct string host_name = string("ec2-22-4-33-234.amazon.com");
    mbuf_write_string(mbuf,&host_name);

	//struct string data = string("12435345,12423523532,1,STARTING,ec2-22-4-33-234.amazon.com|124343451,1242352334444,0,RUNNING,ec2-22-5-31-34.amazon.com");

	for (i = 0; i < nelem; i++) {
	    struct server *peer = (struct server *) array_get(peers, i);
	    if (peer->is_local)
	    	continue;

	    log_debug(LOG_VVERB, "Gossiping to node  '%.*s'", peer->name.len, peer->name.data);

		struct conn * conn = dnode_peer_conn(peer);
		if (conn == NULL) {
		    //running out of connection due to memory exhaust
	        log_debug(LOG_DEBUG, "Unable to obtain a connection object");
		    return DN_ERROR;
		}

	    status = dnode_peer_connect(sp->ctx, peer, conn);
	    if (status != DN_OK ) {
	         dnode_peer_close(sp->ctx, conn);
	         log_debug(LOG_DEBUG, "Error happened in connecting on conn %d", conn->sd);
	         return DN_OK;
	    }



		peer_gossip_forward(sp->ctx, conn, sp->redis, mbuf);
	    //peer_gossip_forward1(sp->ctx, conn, sp->redis, &data);
	}

	return DN_OK;
}


static void
dnode_peer_relink_conn_owner(struct server_pool *sp) {
        struct array *peers = &sp->peers;
  
        uint32_t i,nelem; 
        nelem = array_n(peers);
        for (i = 0; i < nelem; i++) {
                struct server *peer = (struct server *) array_get(peers, i);
                struct conn *conn, *nconn;
                for (conn = TAILQ_FIRST(&peer->s_conn_q); conn != NULL;
                      conn = nconn) {
                      nconn = TAILQ_NEXT(conn, conn_tqe);
                      conn->owner = peer; //re-link to the owner in case of an resize/allocation
                }
        }
         
}

static rstatus_t
dnode_peer_add_node(struct server_pool *sp, struct node *node)
{
	 rstatus_t status;

	 struct array *peers = &sp->peers;
	 struct server *s = array_push(peers);

	 s->owner = sp;

         uint32_t i,nelem;
	 s->idx = array_idx(peers, s);

	 string_copy(&s->pname, node->pname.data, node->pname.len);
	 string_copy(&s->name, node->name.data, node->name.len);
	 string_copy(&s->dc, node->dc.data, node->dc.len);

	 s->port = (uint16_t) node->port;
	 s->is_local = node->is_local;

	 nelem = array_n(&node->tokens);
	 array_init(&s->tokens, nelem, sizeof(struct dyn_token));
	 for (i = 0; i < nelem; i++) {
		struct dyn_token *src_token = (struct dyn_token *) array_get(&node->tokens, i);
		struct dyn_token *dst_token = array_push(&s->tokens);
		copy_dyn_token(src_token, dst_token);
	 }

	 struct sockinfo  *info =  dn_alloc(sizeof(*info)); //need to free this
	 dn_resolve(&s->name, s->port, info);
	 s->family = info->family;
	 s->addrlen = info->addrlen;
	 s->addr = (struct sockaddr *)&info->addr;  //TODOs: fix this by copying, not reference
	 s->ns_conn_q = 0;
	 TAILQ_INIT(&s->s_conn_q);

	 s->next_retry = 0LL;
	 s->failure_count = 0;
	 s->is_seed = node->is_seed;

	 log_debug(LOG_VERB, "add a node to peer %"PRIu32" '%.*s'",
	   		   s->idx, s->pname.len, s->pname.data);

         dnode_peer_relink_conn_owner(sp);

	 status = dnode_peer_pool_run(sp);
         if (status != DN_OK)
            return status;

	 status = dnode_peer_each_preconnect(s, NULL);

	 return status;
}

rstatus_t
dnode_peer_add(struct server_pool *sp, struct node *node)
{
	rstatus_t status;

	log_debug(LOG_VVERB, "dyn: peer has an added message '%.*s'", node->name.len, node->name.data);
	status = dnode_peer_add_node(sp, node);

	return status;
}



rstatus_t
dnode_peer_add_dc(struct server_pool *sp, struct node *node)
{
	rstatus_t status;
	log_debug(LOG_VVERB, "dyn: peer has an add-dc message '%.*s'", node->name.len, node->name.data);
	status = dnode_peer_add_node(sp, node);

	return status;
}


rstatus_t
dnode_peer_remove(struct server_pool *sp, struct node *node)
{
	//rstatus_t status;
	log_debug(LOG_VVERB, "dyn: peer has a removed message '%.*s'", node->name.len, node->name.data);
	return DN_OK;
}


rstatus_t
dnode_peer_replace(struct server_pool *sp, struct node *node)
{
    //rstatus_t status;
    log_debug(LOG_VVERB, "dyn: peer has a replaced message '%.*s'", node->name.len, node->name.data);
    struct array *peers = &sp->peers;
    struct server *s = NULL;

    uint32_t i,nelem;
    //bool node_exist = false;
    //TODOs: use hash table here
    for (i=1, nelem = array_n(peers); i< nelem; i++) {
       struct server * peer = (struct server *) array_get(peers, i);
       if (string_compare(&peer->dc, &node->dc) == 0) {
    	   //TODOs: now only compare 1st token and support vnode later - use hash string on a tokens for comparison
           struct dyn_token *ptoken = (struct dyn_token *) array_get(&peer->tokens, 0);
           struct dyn_token *ntoken = (struct dyn_token *) array_get(&node->tokens, 0);

           if (cmp_dyn_token(ptoken, ntoken) == 0) {
              s = peer; //found a node to replace
           }
       }
    }


    if (s != NULL) {
    	log_debug(LOG_INFO, "Found an old node to replace '%.*s'", s->name.len, s->name.data);
    	log_debug(LOG_INFO, "Replace with address '%.*s'", node->name.len, node->name.data);

        string_deinit(&s->pname);
        string_deinit(&s->name);
        string_copy(&s->pname, node->pname.data, node->pname.len);
        string_copy(&s->name, node->name.data, node->name.len);

        //TODOs: need to free the previous s->addr?
        //if (s->addr != NULL) {
        //   dn_free(s->addr);
        //}

        struct sockinfo  *info =  dn_alloc(sizeof(*info)); //need to free this
        dn_resolve(&s->name, s->port, info);
        s->family = info->family;
        s->addrlen = info->addrlen;
        s->addr = (struct sockaddr *)&info->addr;  //TODOs: fix this by copying, not reference


        dnode_peer_each_disconnect(s, NULL);
        dnode_peer_each_preconnect(s, NULL);
    } else {
    	log_debug(LOG_INFO, "Unable to find any node matched the token");
    }

	return DN_OK;
}


rstatus_t
dnode_peer_connect(struct context *ctx, struct server *server, struct conn *conn)
{
	rstatus_t status;

	ASSERT(!conn->dnode_server && !conn->dnode_client);

	if (conn->sd > 0) {
		/* already connected on peer connection */
		return DN_OK;
	}

	log_debug(LOG_VVERB, "dyn: connect to peer '%.*s'", server->pname.len,
			server->pname.data);

	conn->sd = socket(conn->family, SOCK_STREAM, 0);
	if (conn->sd < 0) {
		log_error("dyn: socket for peer '%.*s' failed: %s", server->pname.len,
				server->pname.data, strerror(errno));
		status = DN_ERROR;
		goto error;
	}

	status = dn_set_nonblocking(conn->sd);
	if (status != DN_OK) {
		log_error("dyn: set nonblock on s %d for peer '%.*s' failed: %s",
				conn->sd,  server->pname.len, server->pname.data,
				strerror(errno));
		goto error;
	}

	if (server->pname.data[0] != '/') {
		status = dn_set_tcpnodelay(conn->sd);
		if (status != DN_OK) {
			log_warn("dyn: set tcpnodelay on s %d for peer '%.*s' failed, ignored: %s",
					conn->sd, server->pname.len, server->pname.data,
					strerror(errno));
		}
	}

	status = event_add_conn(ctx->evb, conn);
	if (status != DN_OK) {
		log_error("dyn: event add conn s %d for peer '%.*s' failed: %s",
				conn->sd, server->pname.len, server->pname.data,
				strerror(errno));
		goto error;
	}

	ASSERT(!conn->connecting && !conn->connected);

	status = connect(conn->sd, conn->addr, conn->addrlen);
	if (status != DN_OK) {
		if (errno == EINPROGRESS) {
			conn->connecting = 1;
			log_debug(LOG_DEBUG, "dyn: connecting on s %d to peer '%.*s'",
					conn->sd, server->pname.len, server->pname.data);
			return DN_OK;
		}

		log_error("dyn: connect on s %d to peer '%.*s' failed: %s", conn->sd,
				server->pname.len, server->pname.data, strerror(errno));

		goto error;
	}

	ASSERT(!conn->connecting);
	conn->connected = 1;
	log_debug(LOG_INFO, "dyn: connected on s %d to peer '%.*s'", conn->sd,
			server->pname.len, server->pname.data);

	return DN_OK;

	error:
	conn->err = errno;
	return status;
}

void
dnode_peer_connected(struct context *ctx, struct conn *conn)
{
	struct server *server = conn->owner;

	ASSERT(!conn->dnode_server && !conn->dnode_client);
	ASSERT(conn->connecting && !conn->connected);

	//fix me
	//stats_server_incr(ctx, server, server_connections);

	conn->connecting = 0;
	conn->connected = 1;

	log_debug(LOG_INFO, "dyn: peer connected on s %d to server '%.*s'", conn->sd,
			server->pname.len, server->pname.data);
}

void
dnode_peer_ok(struct context *ctx, struct conn *conn)
{
	struct server *server = conn->owner;

	ASSERT(!conn->dnode_server && !conn->dnode_client);
	ASSERT(conn->connected);

	if (server->failure_count != 0) {
		log_debug(LOG_VERB, "dyn: reset peer '%.*s' failure count from %"PRIu32
				" to 0", server->pname.len, server->pname.data,
				server->failure_count);
		server->failure_count = 0;
		server->next_retry = 0LL;
	}
}

rstatus_t
dnode_peer_pool_update(struct server_pool *pool)
{
	rstatus_t status;
	int64_t now;
	uint32_t pnlive_server; /* prev # live server */

	//fix me
	if (!pool->auto_eject_hosts) {
		return DN_OK;
	}

	//fix me
	if (pool->next_rebuild == 0LL) {
		return DN_OK;
	}

	now = dn_usec_now();
	if (now < 0) {
		return DN_ERROR;
	}

	//fix me
	if (now <= pool->next_rebuild) {
		if (pool->nlive_server == 0) {
			errno = ECONNREFUSED;
			return DN_ERROR;
		}
		return DN_OK;
	}

	//fixe me to use anotehr variable
	pnlive_server = pool->nlive_server;

	status = dnode_peer_pool_run(pool);
	if (status != DN_OK) {
		log_error("dyn: updating peer pool %"PRIu32" with dist %d failed: %s", pool->idx,
				pool->dist_type, strerror(errno));
		return status;
	}

	log_debug(LOG_INFO, "dyn: update peer pool %"PRIu32" '%.*s' to add %"PRIu32" servers",
			pool->idx, pool->name.len, pool->name.data,
			pool->nlive_server - pnlive_server);


	return DN_OK;
}

static struct dyn_token *
dnode_peer_pool_hash(struct server_pool *pool, uint8_t *key, uint32_t keylen)
{
	ASSERT(array_n(&pool->peers) != 0);
	ASSERT(key != NULL && keylen != 0);

	struct dyn_token *token = dn_alloc(sizeof(struct dyn_token));
	if (token == NULL) {
		return NULL;
	}
	init_dyn_token(token);

	rstatus_t status = pool->key_hash((char *)key, keylen, token);
	if (status != DN_OK) {
		dn_free(token);
		return NULL;
	}

	return token;
}

static struct server *
dnode_peer_pool_server(struct server_pool *pool, struct datacenter *dc, uint8_t *key, uint32_t keylen)
{
	struct server *server;
	uint32_t hash, idx;
	struct dyn_token *token = NULL;

	ASSERT(array_n(&pool->peers) != 0);
	ASSERT(key != NULL && keylen != 0);

	ASSERT(dc != NULL);

	switch (pool->dist_type) {
	case DIST_KETAMA:
		token = dnode_peer_pool_hash(pool, key, keylen);
		hash = token->mag[0];
		idx = ketama_dispatch(dc->continuum, dc->ncontinuum, hash);
		break;

	case DIST_VNODE:
		if (keylen == 0) {
			idx = 0; //for no argument command
			break;
		}
		token = dnode_peer_pool_hash(pool, key, keylen);
		//print_dyn_token(token, 1);
		idx = vnode_dispatch(dc->continuum, dc->ncontinuum, token);
		break;

	case DIST_MODULA:
		token = dnode_peer_pool_hash(pool, key, keylen);
		hash = token->mag[0];
		idx = modula_dispatch(dc->continuum, dc->ncontinuum, hash);
		break;

	case DIST_RANDOM:
		idx = random_dispatch(dc->continuum, dc->ncontinuum, 0);
		break;

	case DIST_SINGLE:
		idx = 0;
		break;

	default:
		NOT_REACHED();
		return NULL;
	}

        //TODOs: should reuse the token
	if (token != NULL) {
		deinit_dyn_token(token);
		dn_free(token);
	}
	ASSERT(idx < array_n(&pool->peers));

	server = array_get(&pool->peers, idx);

	log_debug(LOG_VERB, "dyn: key '%.*s' on dist %d maps to server '%.*s'", keylen,
			key, pool->dist_type, server->pname.len, server->pname.data);

	return server;
}

struct conn *
dnode_peer_pool_conn(struct context *ctx, struct server_pool *pool, struct datacenter *dc,
		             uint8_t *key, uint32_t keylen, uint8_t msg_type)
{
	rstatus_t status;
	struct server *server;
	struct conn *conn;

	//status = dnode_peer_pool_update(pool);
	status = dnode_peer_pool_run(pool);
	if (status != DN_OK) {
            return NULL;
	}

	if (msg_type == 1) {  //always local
        server = array_get(&pool->peers, 0);
	} else {
	    /* from a given {key, keylen} pick a server from pool */
	    server = dnode_peer_pool_server(pool, dc, key, keylen);
	    if (server == NULL) {
                return NULL;
	    }
	}

	/* pick a connection to a given server */
	conn = dnode_peer_conn(server);
	if (conn == NULL) {
            return NULL;
	}

	status = dnode_peer_connect(ctx, server, conn);
	if (status != DN_OK) {
           dnode_peer_close(ctx, conn);
           return NULL;
	}

	return conn;
}

static rstatus_t
dnode_peer_pool_each_preconnect(void *elem, void *data)
{
	rstatus_t status;
	struct server_pool *sp = elem;

	if (!sp->preconnect) {
		return DN_OK;
	}

	status = array_each(&sp->peers, dnode_peer_each_preconnect, NULL);
	if (status != DN_OK) {
		return status;
	}

	return DN_OK;
}

rstatus_t
dnode_peer_pool_preconnect(struct context *ctx)
{
	rstatus_t status;

	status = array_each(&ctx->pool, dnode_peer_pool_each_preconnect, NULL);
	if (status != DN_OK) {
		return status;
	}

	return DN_OK;
}

static rstatus_t
dnode_peer_pool_each_disconnect(void *elem, void *data)
{
	rstatus_t status;
	struct server_pool *sp = elem;

	status = array_each(&sp->peers, dnode_peer_each_disconnect, NULL);
	if (status != DN_OK) {
		return status;
	}

	return DN_OK;
}

void
dnode_peer_pool_disconnect(struct context *ctx)
{
	array_each(&ctx->pool, dnode_peer_pool_each_disconnect, NULL);
}

static rstatus_t
dnode_peer_pool_each_set_owner(void *elem, void *data)
{
	struct server_pool *sp = elem;
	struct context *ctx = data;

	sp->ctx = ctx;

	return DN_OK;
}

rstatus_t
dnode_peer_pool_run(struct server_pool *pool)
{
	ASSERT(array_n(&pool->peers) != 0);

	switch (pool->dist_type) {
	case DIST_KETAMA:
		return ketama_update(pool);

	case DIST_VNODE:
		return vnode_update(pool);

	case DIST_MODULA:
		return modula_update(pool);

	case DIST_RANDOM:
		return random_update(pool);

	default:
		NOT_REACHED();
		return DN_ERROR;
	}

	return DN_OK;
}


static rstatus_t
dc_deinit(void *elem, void *data)
{
	struct datacenter *dc = elem;
	return datacenter_deinit(dc);
}

void
dnode_peer_pool_deinit(struct array *server_pool)
{
	uint32_t i, npool;

	for (i = 0, npool = array_n(server_pool); i < npool; i++) {
		struct server_pool *sp;

		sp = array_pop(server_pool);
		ASSERT(sp->p_conn == NULL);
		//fixe me to use different variables
		ASSERT(TAILQ_EMPTY(&sp->c_conn_q) && sp->dn_conn_q == 0);


		dnode_peer_deinit(&sp->peers);
		array_each(&sp->datacenter, dc_deinit, NULL);
		sp->nlive_server = 0;

		log_debug(LOG_DEBUG, "dyn: deinit peer pool %"PRIu32" '%.*s'", sp->idx,
				sp->name.len, sp->name.data);
	}

	//array_deinit(server_pool);

	log_debug(LOG_DEBUG, "deinit %"PRIu32" peer pools", npool);
}

