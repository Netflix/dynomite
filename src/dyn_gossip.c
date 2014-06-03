/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */


#include <stdlib.h>
#include <unistd.h>
#include <unistd.h>


#include "dyn_core.h"
#include "dyn_server.h"
#include "dyn_gossip.h"
#include "dyn_dnode_peer.h"
#include "dyn_stats.h"
#include "dyn_string.h"
#include "hashkit/dyn_token.h"
#include "seedsprovider/dyn_seeds_provider.h"


static struct gossip_node_pool gn_pool;
static  void* ss = "hello main, from gossippppppppppppppppppp";


static rstatus_t
parse_seeds(struct string *seeds, struct string *dc_name, struct string *port_str,
		    struct string *address, struct array * ptokens)
{
    rstatus_t status;
    uint8_t *p, *q, *start;
    uint8_t *pname, *addr, *port, *dc, *tokens;
    uint32_t k, delimlen, pnamelen, addrlen, portlen, dclen, tokenslen;
    char delim[] = ":::";

    /* parse "hostname:port:dc:tokens" */
    p = seeds->data + seeds->len - 1;
    start = seeds->data;
    addr = NULL;
    addrlen = 0;
    dc = NULL;
    dclen = 0;
    tokens = NULL;
    tokenslen = 0;
    port = NULL;
    portlen = 0;
    delimlen = 3;

    for (k = 0; k < sizeof(delim)-1; k++) {

        q = nc_strrchr(p, start, delim[k]);

        switch (k) {

        case 0:
            tokens = q + 1;
            tokenslen = (uint32_t)(p - tokens + 1);
            log_debug(LOG_VERB, "tokens :::::: '%.*s'", tokenslen, tokens);
            break;

        case 1:
            dc = q + 1;
            dclen = (uint32_t)(p - dc + 1);
            string_copy(dc_name, dc, dclen);
            break;

        case 2:
            port = q + 1;
            portlen = (uint32_t)(p - port + 1);
            string_copy(port_str, port, portlen);
            break;

        default:
            NOT_REACHED();
        }

        p = q - 1;
    }

    if (k != delimlen) {
        return GOS_ERROR;
    }

    pname = seeds->data;
    pnamelen = seeds->len - (tokenslen + dclen + 2);
    status = string_copy(address, pname, pnamelen);

    uint8_t *t_end = tokens + tokenslen;
    status = derive_tokens(ptokens, tokens, t_end);
    if (status != NC_OK) {
        return GOS_ERROR;
    }

    addr = start;
    addrlen = (uint32_t)(p - start + 1);

    /*
    if (value->data[0] != '/') {
        field->port = nc_atoi(port, portlen);
        if (field->port < 0 || !nc_valid_port(field->port)) {
            return "has an invalid port in \"hostname:port:weight [name]\" format string";
        }
    }
    */

    status = string_copy(&address, addr, addrlen);
    if (status != NC_OK) {
        return GOS_ERROR;
    }

    //status = nc_resolve(&address, field->port, &field->info);
    //if (status != NC_OK) {
    //    string_deinit(&address);
    //    return CONF_ERROR;
    //}

    return GOS_OK;
}



static void *
gossip_loop(void *arg)
{
	struct server_pool *sp = arg;
	struct string dc_name;
	struct string port_str;
	struct string address;
	struct array tokens;
	struct string seeds;
	struct string temp;

	string_init(&dc_name);
	string_init(&port_str);
	string_init(&address);

	string_init(&seeds);

	for(;;) {
		usleep(2000000);
		loga("Running  gossip ...");

		if (gn_pool.seeds_provider(NULL, &seeds) == NC_OK) {
		   log_debug(LOG_VERB, "Seeds :::::: '%.*s'", seeds.len, seeds.data);
		   uint8_t *p, *q, *start;
		   start = seeds.data;
		   p = seeds.data + seeds.len - 1;
		   q = nc_strrchr(p, start, '|');

		   uint8_t *seed_node;
		   uint32_t seed_node_len;

		   while (q > start) {
		      seed_node = q + 1;
		      seed_node_len = (uint32_t)(p - seed_node + 1);
		      string_copy(&temp, seed_node, seed_node_len);
		      array_init(&tokens, 1, sizeof(struct dyn_token));
              parse_seeds(&temp, &dc_name, &port_str, &address, &tokens);
              log_debug(LOG_VERB, "dc_name :::::: '%.*s'", dc_name.len, dc_name.data);
              log_debug(LOG_VERB, "port_str :::::: '%.*s'", port_str.len, port_str.data);
              log_debug(LOG_VERB, "address :::::: '%.*s'", address.len, address.data);
              log_debug(LOG_VERB, "tokens size :::: %d", array_n(&tokens));

		      p = q - 1;
		      q = nc_strrchr(p, start, '|');
		      string_deinit(&temp);
		      array_deinit(&tokens);
		  	  string_deinit(&dc_name);
		      string_deinit(&port_str);
		  	  string_deinit(&address);
		   }

		   if (q == NULL) {
		      seed_node_len = (uint32_t)(p - start + 1);
		      seed_node = start;
		      //log_debug(LOG_VERB, "last one :::::: '%.*s'", seed_node_len, seeds.data);
		      string_copy(&temp, seed_node, seed_node_len);
		      array_init(&tokens, 1, sizeof(struct dyn_token));
		      parse_seeds(&temp, &dc_name, &port_str, &address, &tokens);
		      log_debug(LOG_VERB, "dc_name :::::: '%.*s'", dc_name.len, dc_name.data);
		      log_debug(LOG_VERB, "port_str :::::: '%.*s'", port_str.len, port_str.data);
		      log_debug(LOG_VERB, "address :::::: '%.*s'", address.len, address.data);
		      log_debug(LOG_VERB, "tokens size :::: %d", array_n(&tokens));

		      string_deinit(&temp);
		      array_deinit(&tokens);
		   	  string_deinit(&dc_name);
		      string_deinit(&port_str);
		      string_deinit(&address);
		   }
		   string_deinit(&seeds);
		}

		loga("From gossip thread, Leng of C2G_InQ ::: %d", CBUF_Len( C2G_InQ ));

	     while (!CBUF_IsEmpty(C2G_InQ)) {
		     char* s = (char*) CBUF_Pop( C2G_InQ );
		     loga("Gossip: %s", s);
		     //nc_free(s);
	     }

		 //void* ss = "hello main, from gossip";
		 CBUF_Push( C2G_OutQ, ss );


	}

    //event_loop_stats(gossip_loop_callback, arg);

    return NULL;
}

/*
static void
gossip_loop_callback(void *arg1, void *arg2)
{
    struct stats *st = arg1;
    int n = *((int *)arg2);


    stats_aggregate(st);

    if (n == 0) {
        return;
    }


    stats_send_rsp(st);
}
*/


static rstatus_t
gossip_start(struct server_pool *sp)
{
    rstatus_t status;
    pthread_t tid;

    status = pthread_create(&tid, NULL, gossip_loop, sp);
    if (status < 0) {
        log_error("gossip service create failed: %s", strerror(status));
        return NC_ERROR;
    }

    return NC_OK;
}

static void
gossip_stop(struct stats *st)
{
   loga("Stop gossip");
}

static void
gossip_set_seeds_provider(struct string * seeds_provider_str)
{
	//need to check seeds_provider_str == 'florida_seeds_provider'
    gn_pool.seeds_provider = florida_get_seeds;
}



static rstatus_t
gossip_pool_each_init(void *elem, void *data)
{
    rstatus_t status;
    struct server_pool *sp = elem;

    gn_pool.ctx = sp->ctx;
    gn_pool.name = &sp->name;
    gn_pool.idx = sp->idx;
    gn_pool.g_interval = sp->g_interval;

    gossip_set_seeds_provider(&sp->seed_provider);

    int n_datacenter = array_n(&sp->datacenter);
    ASSERT(n_datacenter != 0);

    status = array_init(&gn_pool.datacenters, n_datacenter, sizeof(struct gossip_dc));
    if (status != NC_OK) {
        return status;
    }


    uint32_t i, nelem;
    for (i = 0, nelem = array_n(&sp->datacenter); i < nelem; i++) {
    	struct datacenter *elem = (struct datacenter *) array_get(&sp->datacenter, i);
    	struct gossip_dc *g_dc = array_push(&gn_pool.datacenters);
    	//g_dc->name = elem->name;
    	string_copy(&g_dc->name, elem->name->data, elem->name->len);
    	g_dc->nnodes = elem->ncontinuum;
    	status = array_init(&g_dc->nodes, 1, sizeof(struct node));
    }

    for (i = 0, nelem = array_n(&sp->peers); i < nelem; i++) {
    	struct server *peer = (struct server *) array_get(&sp->peers, i);
    	//better to have a hash table here
    	uint32_t j, ndc;
    	for(j = 0, ndc = array_n(&gn_pool.datacenters); j < ndc; j++) {
            struct gossip_dc *g_dc = (struct gossip_dc *) array_get(&gn_pool.datacenters, j);
    		//log_debug(LOG_DEBUG, "DC2 = '%.*s'", g_dc->name->len, g_dc->name->data);

            if (string_compare(&peer->dc, &g_dc->name) == 0) {
    			struct node *gnode = array_push(&g_dc->nodes);
    			gnode->dc = g_dc;
    			//adding stuff into gossip structure
    			uint32_t ntokens = array_n(&peer->tokens);
    			status = array_init(&gnode->tokens, ntokens, sizeof(struct dyn_token));
                uint32_t k;
                for(k = 0; k < ntokens; k++) {
                	struct dyn_token *ptoken = (struct dyn_token *) array_get(&peer->tokens, k);
                	struct dyn_token *gtoken = array_push(&gnode->tokens);
		            init_dyn_token(gtoken);
                	copy_dyn_token(ptoken, gtoken);
                }
                //copy socket stuffs
    		}
    	}
    }


    /*
    for (i = 0, nelem = array_n(&gn_pool.datacenters); i < nelem; i++) {
    	struct gossip_dc *g_dc = (struct gossip_dc *) array_get(&gn_pool.datacenters, i);
    	loga("num nodes in DC %d", array_n(&g_dc->nodes));
    	int jj;
    	for(jj =0; jj<array_n(&g_dc->nodes); jj++) {
    		struct node * node = (struct node *) array_get(&g_dc->nodes, jj);
    		loga("num tokens : %d", array_n(&node->tokens));
    	}
    }

    log_debug(LOG_VERB, "Seeed provider :::::: '%.*s'",
                  sp->seed_provider.len, sp->seed_provider.data);

    */
    struct stats *st;

    status = gossip_start(sp);
    if (status != NC_OK) {
        goto error;
    }


    return st;

error:
    gossip_destroy(st);
    return NULL;

    //loga("seed provider....................... %s", sp->name);
    //if (!sp->preconnect) {
    //    return NC_OK;
    //}

    //status = array_each(&sp->peers, dnode_peer_each_preconnect, NULL);
    //if (status != NC_OK) {
    //    return status;
    //}

    return NC_OK;
}


rstatus_t
gossip_pool_init(struct context *ctx)
{
    rstatus_t status;

    status = array_each(&ctx->pool, gossip_pool_each_init, NULL);
    if (status != NC_OK) {
        return status;
    }

    return NC_OK;
}


void gossip_pool_deinit(struct context *ctx)
{

}


void
gossip_destroy(struct stats *st)
{
    /*
	stats_stop_aggregator(st);
    stats_pool_unmap(&st->sum);
    stats_pool_unmap(&st->shadow);
    stats_pool_unmap(&st->current);
    stats_destroy_buf(st);
    nc_free(st);
    */
}
