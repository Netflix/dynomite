/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */


#include <stdlib.h>
#include <stdlib.h>
#include <unistd.h>
#include <unistd.h>
#include <ctype.h>

#include "dyn_core.h"
#include "dyn_dict.h"
#include "dyn_dnode_peer.h"
#include "dyn_gossip.h"
#include "dyn_node_snitch.h"
#include "dyn_mbuf.h"
#include "dyn_ring_queue.h"
#include "dyn_server.h"
#include "dyn_string.h"
#include "dyn_token.h"
#include "dyn_util.h"

#include "seedsprovider/dyn_seeds_provider.h"

static const struct string PEER_PORT = string("8101");
static const struct string PEER_SSL_PORT = string("8103");


static void gossip_debug(void);
struct gossip_node_pool gn_pool;
static uint32_t node_count = 0;
static struct node *current_node = NULL;
static struct mbuf * seeds_buf = NULL;

static unsigned int
dict_node_hash(const void *key)
{
    struct node *node = key;
    if (node == NULL)
       return 0;
    return dictGenHashFunction((unsigned char*)node->dc.data, node->dc.len) +
           dictGenHashFunction((unsigned char*)node->rack.data, node->rack.len) +
           node->token.mag[0];
}

static int
dict_node_key_compare(void *privdata, const void *key1, const void *key2)
{
    DICT_NOTUSED(privdata);
    struct node *node1 = key1;
    struct node *node2 = key2;

    ASSERT(node1 == NULL || node2 == NULL);

    return (string_compare(&node1->dc, &node2->dc) == 0) &&
           (string_compare(&node1->rack, &node2->rack) == 0) &&
           (cmp_dyn_token(&node1->token, &node2->token) == 0);

}

int
dict_string_key_compare(void *privdata, const void *key1, const void *key2)
{
    DICT_NOTUSED(privdata);
    struct string *s1 = key1;
    struct string *s2 = key2;

    //return (s1->len != s2->len)? 0 : strncmp(s1->data, s2->data, s1->len) == 0;
    return string_compare(s1, s2) == 0;
}

static void
dict_node_destructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    struct node *node = val;
    node_deinit(node);
    dn_free(node);
}

void
dict_string_destructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    struct string *s = val;
    string_deinit(s);
    dn_free(s);
}

unsigned int
dict_string_hash(const void *key)
{
    struct string *s = key;
    //return dictGenHashFunction((unsigned char*)key, dn_strlen((char*)key));
    if (s == NULL)
        return 0;
    return dictGenHashFunction(s->data, s->len);
}




dictType token_table_dict_type = {
    dict_string_hash,            /* hash function */
    NULL,                        /* key dup */
    NULL,                        /* val dup */
    dict_string_key_compare,     /* key compare */
    dict_string_destructor,      /* key destructor */
    NULL                         /* val destructor */
};


dictType string_table_dict_type = {
    dict_string_hash,            /* hash function */
    NULL,                        /* key dup */
    NULL,                        /* val dup */
    dict_string_key_compare,     /* key compare */
    dict_string_destructor,      /* key destructor */
    NULL                         /* val destructor */
};


dictType gossip_table_dict_type = {
    dict_node_hash,            /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dict_node_key_compare,     /* key compare */
    dict_node_destructor,      /* key destructor */
    NULL                       /* val destructor */
};



static rstatus_t
gossip_process_msgs(void)
{
    //TODOs: fix this to process an array of nodes
    while (!CBUF_IsEmpty(C2G_InQ)) {
        struct ring_msg *msg = (struct ring_msg *) CBUF_Pop(C2G_InQ);
        msg->cb(msg);
        ring_msg_deinit(msg);
    }

    return DN_OK;
}


static rstatus_t
gossip_msg_to_core(struct server_pool *sp, struct node *node, void *cb)
{
    struct ring_msg *msg = create_ring_msg();
    struct node *rnode = (struct node *) array_get(&msg->nodes, 0);
    node_copy(node, rnode);

    msg->cb = cb;
    msg->sp = sp;
    CBUF_Push(C2G_OutQ, msg);

    return DN_OK;
}

static rstatus_t
gossip_ring_msg_to_core(struct server_pool *sp, struct ring_msg *msg, void *cb)
{
    msg->cb = cb;
    msg->sp = sp;
    CBUF_Push(C2G_OutQ, msg);

    return DN_OK;
}


static void
write_char(uint8_t *pos, char ch)
{
   *pos = ch;
   pos += 1;
}

static int
write_number(uint8_t *pos, uint64_t num, int *count)
{
   if (num < 10) {
      write_char(pos, '0' + (int) num);
      *count = 1;
      return 1;
   }

   write_number(pos, num / 10, count);
   write_char(pos + (*count), '0' + (num % 10));
   *count = *count + 1;
   return count;
}


static void
string_write_uint32(struct string *str, uint32_t num, int pos)
{
    if (num < 10) {
        *(str->data + pos) = '0' + num;
        return;
    }

    *(str->data + pos) = '0' + (num % 10);
    string_write_uint32(str, num / 10, pos - 1);
}

static int num_len(uint32_t num) {
    if (num < 10) {
        return 1;
    }

    return 1 + num_len(num / 10);
}

static struct string *token_to_string(struct dyn_token *token) {
    uint32_t num = token->mag[0];
    int len = num_len(num);
    struct string *result = dn_alloc(sizeof(*result));
    string_init(result);

    result->data = dn_alloc(sizeof(uint8_t) * len);
    result->len = len;

    string_write_uint32(result, num, result->len - 1);
    return result;
}


//Simple failure detector - will have an advanced version later
static uint8_t
gossip_failure_detector(struct node *node)
{
    log_debug(LOG_VERB, "In gossip_failure_detector");

    if (node == NULL)
        return UNKNOWN;

    if (node->is_local)
        return NORMAL;

    uint64_t cur_ts = (uint64_t) time(NULL);
    uint64_t delta = gn_pool.g_interval/1000 * 40; //g_internal is in milliseconds

    //loga("cur_ts %d", cur_ts);
    //loga("delta %d", delta);
    //loga("node->ts = %d", node->ts);
    //loga("node state = %d", node->state);

    if (cur_ts - node->ts > delta) { //if there is no update for delta time
        return DOWN;
    }

    return node->state;
}


static rstatus_t
gossip_forward_state(struct server_pool *sp)
{
    //assume each record needs maximum 256 bytes
    struct ring_msg *msg = create_ring_msg_with_data(256 * node_count);
    uint8_t *data = msg->data; //dn_zalloc(sizeof(uint8_t) * 256 * node_count);//msg->data;
    uint8_t *pos = data;
    int i = 0;

    dictIterator *dc_it;
    dictEntry *dc_de;
    dc_it = dictGetIterator(gn_pool.dict_dc);
    while ((dc_de = dictNext(dc_it)) != NULL) {
        struct gossip_dc *g_dc = dictGetVal(dc_de);
        //log_debug(LOG_VERB, "\tDC name           : '%.*s'", g_dc->name.len, g_dc->name.data);
        dictIterator *rack_it = dictGetIterator(g_dc->dict_rack);
        dictEntry *rack_de;
        while ((rack_de = dictNext(rack_it)) != NULL) {
            struct gossip_rack *g_rack = dictGetVal(rack_de);
            //log_debug(LOG_VERB, "\tRack name           : '%.*s'", g_rack->name.len, g_rack->name.data);

            dictIterator *node_it = dictGetIterator(g_rack->dict_token_nodes);
            dictEntry *node_de;
            while ((node_de = dictNext(node_it)) != NULL) {
                struct node *gnode = dictGetVal(node_de);
                //log_debug(LOG_VERB, "\tNode name           : '%.*s'", gnode->name.len, gnode->name.data);

                if (i++ > 0) {
                    //pipe separator
                    *pos = '|';
                    pos += 1;
                }

                //write dc name
                dn_memcpy(pos, g_dc->name.data, g_dc->name.len);
                pos += g_dc->name.len;

                //$ separator
                *pos = '$';
                pos += 1;

                //write rack name
                dn_memcpy(pos, g_rack->name.data, g_rack->name.len);
                pos += g_rack->name.len;

                //$ separator
                *pos = '$';
                pos += 1;

                //write node token
                struct string *token_str = dictGetKey(node_de);
                //log_debug(LOG_VERB, "\tToken string          : '%.*s'", token_str->len, token_str->data);
                int k;
                for(k=0; k<token_str->len;k++, pos++) {
                    *pos = *(token_str->data + k);
                }

                //comma separator
                *pos = ',';
                pos += 1;

                //write ts
                int count = 0;
                uint64_t ts;
                if (gnode->is_local)  //only update my own timestamp
                    ts = (uint64_t) time(NULL);
                else ts = gnode->ts;

                count = 0;
                write_number(pos, ts, &count);
                pos += count;

                //comma separator
                *pos = ',';
                pos += 1;

                //write state
                uint8_t new_state = gossip_failure_detector(gnode);
                gnode->state = new_state;
                count = 0;
                write_number(pos, gnode->state, &count);
                pos += count;

                //comma separator
                *pos = ',';
                pos += 1;

                //write addresss
                for(k=0; k<gnode->name.len; k++, pos++) {
                    *pos = *(gnode->name.data + k);
                }

            }
            dictReleaseIterator(node_it);
        }
        dictReleaseIterator(rack_it);
    }

    msg->len = pos-data;

    log_debug(LOG_VERB, "\tForwarding my current gossip states           : '%.*s'", (pos-data), data);

    dictReleaseIterator(dc_it);

    return gossip_ring_msg_to_core(sp, msg, dnode_peer_forward_state);
}


static rstatus_t
gossip_announce_joining(struct server_pool *sp)
{
    return gossip_msg_to_core(sp, NULL, dnode_peer_handshake_announcing);
}

static rstatus_t
parse_seeds(struct string *seeds, struct string *dc_name, struct string *rack_name,
        struct string *port_str, struct string *address, struct string *name,
        struct dyn_token *ptoken)
{
    rstatus_t status;
    uint8_t *p, *q, *start;
    uint8_t *pname, *port, *rack, *dc, *token, *addr;
    uint32_t k, delimlen, pnamelen, portlen, racklen, dclen, tokenlen, addrlen;
    char delim[] = "::::";

    /* parse "hostname:port:rack:dc:tokens" */
    p = seeds->data + seeds->len - 1;
    start = seeds->data;
    rack = NULL;
    dc = NULL;
    racklen = 0;
    dclen = 0;
    token = NULL;
    tokenlen = 0;
    port = NULL;
    portlen = 0;
    delimlen = 4;

    for (k = 0; k < sizeof(delim)-1; k++) {
        q = dn_strrchr(p, start, delim[k]);

        switch (k) {
        case 0:
            token = q + 1;
            tokenlen = (uint32_t)(p - token + 1);
            break;
        case 1:
            dc = q + 1;
            dclen = (uint32_t)(p - dc + 1);
            string_copy(dc_name, dc, dclen);
            break;
        case 2:
            rack = q + 1;
            racklen = (uint32_t)(p - rack + 1);
            string_copy(rack_name, rack, racklen);
            break;

        case 3:
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

    //pname = hostname:port
    pname = seeds->data;
    pnamelen = seeds->len - (tokenlen + racklen + dclen + 3);
    status = string_copy(address, pname, pnamelen);


    //addr = hostname or ip only
    addr = start;
    addrlen = (uint32_t)(p - start + 1);
    //if it is a dns name, convert to IP or otherwise keep that IP
    if (!isdigit( (char) addr[0])) {
        addr[addrlen] = '\0';
        char *local_ip4 = hostname_to_private_ip4( (char *) addr);
        if (local_ip4 != NULL) {
            status = string_copy_c(name, local_ip4);
        } else
            status = string_copy(name, addr, addrlen);
    } else {
        status = string_copy(name, addr, addrlen);
    }
    if (status != DN_OK) {
        return GOS_ERROR;
    }

    uint8_t *t_end = token + tokenlen;
    status = derive_token(ptoken, token, t_end);
    if (status != DN_OK) {
        return GOS_ERROR;
    }

    //status = dn_resolve(&address, field->port, &field->info);
    //if (status != DN_OK) {
    //    string_deinit(&address);
    //    return CONF_ERROR;
    //}

    return GOS_OK;
}

static rstatus_t
gossip_dc_init(struct gossip_dc *g_dc, struct string *dc)
{
    rstatus_t status;
    g_dc->dict_rack = dictCreate(&string_table_dict_type, NULL);
    string_copy(&g_dc->name, dc->data, dc->len);
    status = array_init(&g_dc->racks, 50, sizeof(struct gossip_rack));
    return status;
}


static rstatus_t
gossip_rack_init(struct gossip_rack *g_rack, struct string *dc, struct string *rack)
{
    rstatus_t status;
    g_rack->dict_name_nodes = dictCreate(&string_table_dict_type, NULL);
    g_rack->dict_token_nodes = dictCreate(&token_table_dict_type, NULL);
    string_copy(&g_rack->name, rack->data, rack->len);
    string_copy(&g_rack->dc, dc->data, dc->len);
    g_rack->nnodes = 0;
    g_rack->nlive_nodes = 0;
    status = array_init(&g_rack->nodes, 200, sizeof(struct node));

    return status;
}


static struct node *
gossip_add_node_to_rack(struct server_pool *sp, struct string *dc, struct gossip_rack *g_rack,
        struct string *address, struct string *ip, struct string *port, struct dyn_token *token)
{
    rstatus_t status;
    log_debug(LOG_VERB, "gossip_add_node_to_rack : dc[%.*s] rack[%.*s] address[%.*s] ip[%.*s] port[%.*s]",
            dc->len, dc->data, g_rack->name, address->len, address->data, ip->len, ip->data, port->len, port->data);


    int port_i = dn_atoi(port->data, port->len);
    if (port_i == 0) {
        return NULL; //bad data
    }

    struct node *gnode = (struct node *) array_push(&g_rack->nodes);
    node_init(gnode);
    status = string_copy(&gnode->dc, dc->data, dc->len);
    status = string_copy(&gnode->rack, g_rack->name.data, g_rack->name.len);
    status = string_copy(&gnode->name, ip->data, ip->len);
    status = string_copy(&gnode->pname, address->data, address->len); //ignore the port for now
    IGNORE_RET_VAL(status);
    gnode->port = port_i;

    struct dyn_token * gtoken = &gnode->token;
    copy_dyn_token(token, gtoken);

    g_rack->nnodes++;

    //add into dicts
    dictAdd(g_rack->dict_name_nodes, &gnode->name, gnode);
    dictAdd(g_rack->dict_token_nodes, token_to_string(token), gnode);

    return gnode;
}


static rstatus_t
gossip_add_node(struct server_pool *sp, struct string *dc, struct gossip_rack *g_rack,
        struct string *address, struct string *ip, struct string *port, struct dyn_token *token, uint8_t state)
{
    rstatus_t status;
    log_debug(LOG_VERB, "gossip_add_node : dc[%.*s] rack[%.*s] address[%.*s] ip[%.*s] port[%.*s]",
            dc->len, dc->data, g_rack->name.len, g_rack->name.data, address->len, address->data, ip->len, ip->data, port->len, port->data);

    struct node *gnode = gossip_add_node_to_rack(sp, dc, g_rack, address, ip, port, token);
    if (gnode == NULL) {
        return DN_ENOMEM;
    }

    node_count++;
    gnode->state = state;

    status = gossip_msg_to_core(sp, gnode, dnode_peer_add);
    return status;
}


static rstatus_t
gossip_replace_node(struct server_pool *sp, struct node *node,
        struct string *new_address, struct string *new_ip, uint8_t state)
{
    rstatus_t status;
    log_debug(LOG_WARN, "gossip_replace_node : dc[%.*s] rack[%.*s] oldaddr[%.*s] newaddr[%.*s] newip[%.*s]",
            node->dc, node->rack, node->name, new_address->len, new_address->data, new_ip->len, new_ip->data);

    string_deinit(&node->name);
    string_deinit(&node->pname);
    status = string_copy(&node->name, new_ip->data, new_ip->len);
    status = string_copy(&node->pname, new_address->data, new_address->len);
    //port is supposed to be the same

    node->state = state;
    gossip_msg_to_core(sp, node, dnode_peer_replace);

    //should check for status
    return status;
}



static rstatus_t
gossip_update_state(struct server_pool *sp, struct node *node, uint8_t state, uint64_t timestamp)
{
    rstatus_t status = DN_OK;
    log_debug(LOG_VVERB, "gossip_update_state : dc[%.*s] rack[%.*s] name[%.*s] token[%d] state[%d]",
            node->dc, node->rack, node->name, node->token.mag[0], state);

    if (node->ts < timestamp) {
       node->state = state;
       node->ts = timestamp;
    }

    //gossip_msg_to_core(sp, node, dnode_peer_update_state);

    return status;
}


static rstatus_t
gossip_add_node_if_absent(struct server_pool *sp,
        struct string *dc,
        struct string *rack,
        struct string *address,
        struct string *ip,
        struct string *port,
        struct dyn_token *token,
        uint8_t state,
        uint64_t timestamp)
{
    log_debug(LOG_VERB, "gossip_add_node_if_absent          : '%.*s'", address->len, address->data);

    struct gossip_dc * g_dc = dictFetchValue(gn_pool.dict_dc, dc);
    if (g_dc == NULL) {
        log_debug(LOG_VERB, "We don't have this datacenter? '%.*s' ", dc->len, dc->data);
        g_dc = array_push(&gn_pool.datacenters);
        gossip_dc_init(g_dc, dc);
        dictAdd(gn_pool.dict_dc, &g_dc->name, g_dc);
    } else {
        log_debug(LOG_VERB, "We got a datacenter in dict for '%.*s' ", dc->len, dc->data);
    }

    struct gossip_rack *g_rack = dictFetchValue(g_dc->dict_rack, rack);
    if (g_rack == NULL) {
        log_debug(LOG_VERB, "We don't have this rack? '%.*s' ", rack->len, rack->data);
        g_rack = array_push(&g_dc->racks);
        gossip_rack_init(g_rack, dc, rack);
        dictAdd(g_dc->dict_rack, &g_rack->name, g_rack);
    } else {
        log_debug(LOG_VERB, "We got a rack for '%.*s' ", rack->len, rack->data);
    }

    struct string *token_str = token_to_string(token);
    struct node *g_node = dictFetchValue(g_rack->dict_token_nodes, token_str);

    if (g_node == NULL) { //never existed
        log_debug(LOG_VERB, "Node not found!  We need to add it");
        log_debug(LOG_VERB, "adding node : dc[%.*s]", dc->len, dc->data);
        log_debug(LOG_VERB, "adding node : g_rack[%.*s]", g_rack->name.len, g_rack->name.data);
        log_debug(LOG_VERB, "adding node : address[%.*s]", address->len, address->data);
        log_debug(LOG_VERB, "adding node : ip[%.*s]", ip->len, ip->data);
        log_debug(LOG_VERB, "adding node : port[%.*s]", port->len, port->data);
        log_debug(LOG_VERB, "suggested state : %d", state);
        //print_dyn_token(token, 6);
        gossip_add_node(sp, dc, g_rack, address, ip, port, token, state);
    } else if (dictFind(g_rack->dict_name_nodes, ip) != NULL) {
        log_debug(LOG_VERB, "Node found");
        if (!g_node->is_local) {  //don't update myself here
            if (string_compare(&g_node->name, ip) != 0) {
                log_debug(LOG_WARN, "Replacing an existing token with new info");
                gossip_replace_node(sp, g_node, address, ip, state);
            } else {  //update state
                gossip_update_state(sp, g_node, state, timestamp);
            }
        }
    } else {
        log_debug(LOG_WARN, "Replacing an existing token with new IP or address");
        gossip_replace_node(sp, g_node, address, ip, state);
        dictAdd(g_rack->dict_name_nodes, &g_node->name, g_node);
    }

    //free token_str
    string_deinit(token_str);
    dn_free(token_str);
    return 0;
}


static rstatus_t
gossip_update_seeds(struct server_pool *sp, struct mbuf *seeds)
{
    struct string rack_name;
    struct string dc_name;
    struct string port_str;
    struct string address;
    struct string ip;
    //struct array tokens;
    struct dyn_token token;

    struct string temp;

    string_init(&rack_name);
    string_init(&dc_name);
    string_init(&port_str);
    string_init(&address);
    string_init(&ip);
    init_dyn_token(&token);

    uint8_t *p, *q, *start;
    start = seeds->start;
    p = seeds->last - 1;
    q = dn_strrchr(p, start, '|');

    uint8_t *seed_node;
    uint32_t seed_node_len;

    while (q > start) {
        seed_node = q + 1;
        seed_node_len = (uint32_t)(p - seed_node + 1);
        string_copy(&temp, seed_node, seed_node_len);
        //array_init(&tokens, 1, sizeof(struct dyn_token));
        init_dyn_token(&token);
        parse_seeds(&temp, &dc_name, &rack_name, &port_str, &address, &ip,  &token);
        log_debug(LOG_VERB, "address          : '%.*s'", address.len, address.data);
        log_debug(LOG_VERB, "rack_name         : '%.*s'", rack_name.len, rack_name.data);
        log_debug(LOG_VERB, "dc_name        : '%.*s'", dc_name.len, dc_name.data);
        log_debug(LOG_VERB, "ip         : '%.*s'", ip.len, ip.data);

        //struct dyn_token *token = array_get(&tokens, 0);
        gossip_add_node_if_absent(sp, &dc_name, &rack_name, &address, &ip, &port_str, &token, NORMAL, (uint64_t) time(NULL));

        p = q - 1;
        q = dn_strrchr(p, start, '|');
        string_deinit(&temp);
        //array_deinit(&tokens);
        deinit_dyn_token(&token);
        string_deinit(&rack_name);
        string_deinit(&dc_name);
        string_deinit(&port_str);
        string_deinit(&address);
        string_deinit(&ip);
    }

    if (q == NULL) {
        seed_node_len = (uint32_t)(p - start + 1);
        seed_node = start;

        string_copy(&temp, seed_node, seed_node_len);
        //array_init(&tokens, 1, sizeof(struct dyn_token));
        init_dyn_token(&token);
        parse_seeds(&temp, &dc_name, &rack_name, &port_str, &address, &ip, &token);

        //struct dyn_token *token = array_get(&tokens, 0);
        gossip_add_node_if_absent(sp, &dc_name, &rack_name, &address, &ip, &port_str, &token, NORMAL, (uint64_t) time(NULL));
    }

    string_deinit(&temp);
    //array_deinit(&tokens);
    deinit_dyn_token(&token);
    string_deinit(&rack_name);
    string_deinit(&dc_name);
    string_deinit(&port_str);
    string_deinit(&address);
    string_deinit(&ip);

    gossip_debug();
    return DN_OK;
}


/*static void
gossip_metainfo(void)
{
        dictIterator *dc_it;
        dictEntry *dc_de;
        dc_it = dictGetIterator(gn_pool.dict_dc);
        while ((dc_de = dictNext(dc_it)) != NULL) {
            struct gossip_dc *g_dc = dictGetVal(dc_de);
            log_debug(LOG_VERB, "\tDC name           : '%.*s'", g_dc->name.len, g_dc->name.data);
            dictIterator *rack_it = dictGetIterator(g_dc->dict_rack);
            dictEntry *rack_de;
            while ((rack_de = dictNext(rack_it)) != NULL) {
                struct gossip_rack *g_rack = dictGetVal(rack_de);
                log_debug(LOG_VERB, "\tRack name           : '%.*s'", g_rack->name.len, g_rack->name.data);

                dictIterator *node_it = dictGetIterator(g_rack->dict_token_nodes);
                dictEntry *node_de;
                while ((node_de = dictNext(node_it)) != NULL) {
                    struct node *gnode = dictGetVal(node_de);
                    log_debug(LOG_VERB, "\tNode name           : '%.*s'", gnode->name.len, gnode->name.data);

                    struct string *token_key = dictGetKey(node_de);
                    log_debug(LOG_VERB, "\tNode token           : '%.*s'", *token_key);
                }
            }
        }

}*/

static void *
gossip_loop(void *arg)
{
    struct server_pool *sp = arg;
    uint64_t gossip_interval = gn_pool.g_interval * 1000;

    seeds_buf = mbuf_alloc(SEED_BUF_SIZE);

    log_debug(LOG_VVERB, "gossip_interval : %d msecs", gn_pool.g_interval);
    for(;;) {
        usleep(gossip_interval);

        log_debug(LOG_VERB, "Gossip is running ...");

        if (gn_pool.seeds_provider != NULL && gn_pool.seeds_provider(sp->ctx, seeds_buf) == DN_OK) {
            log_debug(LOG_VERB, "Got seed nodes  '%.*s'", mbuf_length(seeds_buf), seeds_buf->pos);
            gossip_update_seeds(sp, seeds_buf);
        }

        current_node->ts = (uint64_t) time(NULL);
        gossip_process_msgs();

        if (current_node->state == NORMAL) {
            gn_pool.ctx->dyn_state = NORMAL;
        }

        if (!sp->ctx->enable_gossip) {
            //gossip_debug();
            continue;  //no gossiping
        }

        if (node_count == 1) { //single node deployment
            gn_pool.ctx->dyn_state = NORMAL;
            continue;
        }

        //STANDBY state for warm bootstrap
        if (gn_pool.ctx->dyn_state == STANDBY)
            continue;

        if (gn_pool.ctx->dyn_state == JOINING) {
            log_debug(LOG_NOTICE, "I am still joining the ring!");
            //aggressively contact all known nodes before changing to state NORMAL
            gossip_announce_joining(sp);
            usleep(MAX(gn_pool.ctx->timeout, gossip_interval) * 2);
        } else if (gn_pool.ctx->dyn_state == NORMAL) {
            gossip_forward_state(sp);
        }

        gossip_debug();

    } //end for loop

    mbuf_dealloc(seeds_buf);
    seeds_buf = NULL;

    return NULL;
}


rstatus_t
gossip_start(struct server_pool *sp)
{
    rstatus_t status;
    pthread_t tid;

    status = pthread_create(&tid, NULL, gossip_loop, sp);
    if (status < 0) {
        log_error("gossip service create failed: %s", strerror(status));
        return DN_ERROR;
    }

    return DN_OK;
}


static void
gossip_set_seeds_provider(struct string * seeds_provider_str)
{
    log_debug(LOG_VERB, "Seed provider :::::: '%.*s'",
            seeds_provider_str->len, seeds_provider_str->data);

    if (dn_strncmp(seeds_provider_str->data, FLORIDA_PROVIDER, 16) == 0) {
        gn_pool.seeds_provider = florida_get_seeds;
    } else {
        gn_pool.seeds_provider = NULL;
    }
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

    //dictDisableResize();
    gn_pool.dict_dc = dictCreate(&string_table_dict_type, NULL);

    gossip_set_seeds_provider(&sp->seed_provider);

    uint32_t n_dc = array_n(&sp->datacenters);
    if (n_dc == 0)
        return DN_OK;

    if (n_dc > 0) {
        status = array_init(&gn_pool.datacenters, n_dc, sizeof(struct gossip_dc));
        if (status != DN_OK) {
            return status;
        }
    }

    //add racks and datacenters
    uint32_t dc_cnt = array_n(&sp->datacenters);
    uint32_t dc_index;
    for(dc_index = 0; dc_index < dc_cnt; dc_index++) {
        struct datacenter *dc = array_get(&sp->datacenters, dc_index);
        uint32_t rack_cnt = array_n(&dc->racks);
        uint32_t rack_index;
        for(rack_index = 0; rack_index < rack_cnt; rack_index++) {
            struct rack *rack = array_get(&dc->racks, rack_index);

            if (dictFind(gn_pool.dict_dc, rack->dc) == NULL) {
                struct gossip_dc *g_dc = array_push(&gn_pool.datacenters);
                gossip_dc_init(g_dc, rack->dc);
                dictAdd(gn_pool.dict_dc, &g_dc->name, g_dc);
            }

            struct gossip_dc *g_dc = dictFetchValue(gn_pool.dict_dc, rack->dc);
            if (dictFind(g_dc->dict_rack, rack->name) == NULL) {
                log_debug(LOG_VERB, "What?? No rack in Dict for rack         : '%.*s'", g_dc->name);
                struct gossip_rack *g_rack = array_push(&g_dc->racks);
                gossip_rack_init(g_rack, rack->dc, rack->name);
                dictAdd(g_dc->dict_rack, &g_rack->name, g_rack);
            }
        }
    }

    uint32_t i, nelem;
    for (i = 0, nelem = array_n(&sp->peers); i < nelem; i++) {
        struct server *peer = array_get(&sp->peers, i);
        struct gossip_dc *g_dc = dictFetchValue(gn_pool.dict_dc, &peer->dc);
        struct gossip_rack *g_rack = dictFetchValue(g_dc->dict_rack, &peer->rack);
        struct node *gnode = array_push(&g_rack->nodes);

        node_init(gnode);

        string_copy(&gnode->dc, peer->dc.data, peer->dc.len);
        string_copy(&gnode->rack, g_rack->name.data, g_rack->name.len);
        string_copy(&gnode->name, peer->name.data, peer->name.len);
        string_copy(&gnode->pname, peer->pname.data, peer->pname.len); //ignore the port for now
        gnode->port = peer->port;
        gnode->is_local = peer->is_local;


        if (i == 0) { //Don't override its own state
            gnode->state = sp->ctx->dyn_state;  //likely it is JOINING state
            gnode->ts = (uint64_t)time(NULL);
            current_node = gnode;
            char *b_address = get_broadcast_address(sp);
            string_deinit(&gnode->name);
            string_copy(&gnode->name, b_address, dn_strlen(b_address));
        } else {
            gnode->state = DOWN;
            gnode->ts = 1010101;  //make this to be a very aged ts
        }

        struct dyn_token *ptoken = array_get(&peer->tokens, 0);
        copy_dyn_token(ptoken, &gnode->token);

        //copy socket stuffs

        g_rack->nnodes++;
        //add into dicts
        dictAdd(g_rack->dict_name_nodes, &gnode->name, gnode);
        dictAdd(g_rack->dict_token_nodes, token_to_string(&gnode->token), gnode);

        node_count++;

    }

    //gossip_debug();

    status = gossip_start(sp);
    if (status != DN_OK) {
        goto error;
    }

    return DN_OK;

    error:
    gossip_destroy(sp);
    return DN_OK;

}


rstatus_t
gossip_pool_init(struct context *ctx)
{
    rstatus_t status;

    status = array_each(&ctx->pool, gossip_pool_each_init, NULL);
    if (status != DN_OK) {
        return status;
    }

    return DN_OK;
}


void gossip_pool_deinit(struct context *ctx)
{

}


rstatus_t
gossip_destroy(struct server_pool *sp)
{
    return DN_OK;
}


long long dictFingerprint(dict *d);
void gossip_debug(void)
{
    uint32_t i, nelem;
    for (i = 0, nelem = array_n(&gn_pool.datacenters); i < nelem; i++) {
        log_debug(LOG_VERB, "===============Gossip dump===============================");
        struct gossip_dc *g_dc = (struct gossip_dc *) array_get(&gn_pool.datacenters, i);
        log_debug(LOG_VERB, "\tDC name           : '%.*s'", g_dc->name.len, g_dc->name.data);
        log_debug(LOG_VERB, "=========================================================");

        uint32_t k, kelem;
        for (k = 0, kelem = array_n(&g_dc->racks); k < kelem; k++) {
            struct gossip_rack *g_rack = (struct gossip_rack *) array_get(&g_dc->racks, k);

            log_debug(LOG_VERB, "\tRACK name         : '%.*s'", g_rack->name.len, g_rack->name.data);
            log_debug(LOG_VERB, "\tNum nodes in RACK : '%d'", array_n(&g_rack->nodes));
            uint32_t jj;
            for (jj = 0; jj < array_n(&g_rack->nodes); jj++) {
                log_debug(LOG_VERB, "-----------------------------------------");
                struct node *node = (struct node *) array_get(&g_rack->nodes, jj);
                log_debug(LOG_VERB, "\t\tNode name          : '%.*s'", node->name);
                log_debug(LOG_VERB, "\t\tNode pname         : '%.*s'", node->pname);
                log_debug(LOG_VERB, "\t\tNode state         : %"PRIu32"", node->state);
                log_debug(LOG_VERB, "\t\tNode port          : %"PRIu32"", node->port);
                log_debug(LOG_VERB, "\t\tNode is_local      : %"PRIu32" ", node->is_local);
                log_debug(LOG_VERB, "\t\tNode last_retry    : %"PRIu32" ", node->last_retry);
                log_debug(LOG_VERB, "\t\tNode failure_count : %"PRIu32" ", node->failure_count);

                print_dyn_token(&node->token, 8);
                log_debug(LOG_VERB, "\t\tFinger print    : %"PRIu64" ", dictFingerprint(g_rack->dict_token_nodes));

            }
        }
    }
    log_debug(LOG_VERB, "...........................................................");
}


rstatus_t
gossip_msg_peer_update(void *rmsg)
{
    rstatus_t status;
    struct ring_msg *msg = rmsg;
    struct server_pool *sp = msg->sp;

       //TODOs: need to fix this as it is breaking warm bootstrap
    current_node->state = NORMAL;
    sp->ctx->dyn_state = NORMAL;

    int i=0;
    int n = array_n(&msg->nodes);
    for(i=0; i<n; i++) {
        struct node *node = array_get(&msg->nodes, i);
        log_debug(LOG_VVERB, "Processing msg   gossip_msg_peer_update '%.*s'", node->name.len, node->name.data);
        log_debug(LOG_VVERB, "Processing    gossip_msg_peer_update : datacenter '%.*s'", node->dc.len, node->dc.data);
        log_debug(LOG_VVERB, "Processing    gossip_msg_peer_update : rack '%.*s'", node->rack.len, node->rack.data);
        log_debug(LOG_VVERB, "Processing    gossip_msg_peer_update : name '%.*s'", node->name.len, node->name.data);
        log_debug(LOG_VVERB, "State %d", node->state);
        print_dyn_token(&node->token, 10);

        status = gossip_add_node_if_absent(sp, &node->dc, &node->rack,
                                           &node->name, &node->name,
                                           (node->port == 8101)? &PEER_PORT : &PEER_SSL_PORT,
                                           &node->token,
                                           node->state,
                                           node->ts);
    }
    gossip_debug();

    return status;
}
