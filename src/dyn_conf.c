/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 


/*
 * twemproxy - A fast and lightweight proxy for memcached protocol.
 * Copyright (C) 2011 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "dyn_core.h"
#include "dyn_topology.h"
#include "dyn_conf.h"
#include "dyn_server.h"
#include "dyn_dnode_peer.h"

#include "dyn_token.h"
#include "proto/dyn_proto.h"

#define DEFINE_ACTION(_hash, _name) string(#_name),
static struct string hash_strings[] = {
    HASH_CODEC( DEFINE_ACTION )
    null_string
};
#undef DEFINE_ACTION

#define DEFINE_ACTION(_hash, _name) hash_##_name,
static hash_t hash_algos[] = {
    HASH_CODEC( DEFINE_ACTION )
    NULL
};
#undef DEFINE_ACTION

#define DEFINE_ACTION(_dist, _name) string(#_name),
static struct string dist_strings[] = {
    DIST_CODEC( DEFINE_ACTION )
    null_string
};
#undef DEFINE_ACTION

#define CONF_OK             (void *) NULL
#define CONF_ERROR          (void *) "has an invalid value"
#define CONF_ROOT_DEPTH     1
#define CONF_MAX_DEPTH      CONF_ROOT_DEPTH + 1
#define CONF_DEFAULT_ARGS       3
#define CONF_UNSET_NUM  0
#define CONF_UNSET_PTR  NULL
#define CONF_DEFAULT_SERVERS    8
#define CONF_UNSET_HASH (hash_type_t) -1
#define CONF_UNSET_DIST (dist_type_t) -1

#define CONF_DEFAULT_HASH                    HASH_MURMUR
#define CONF_DEFAULT_DIST                    DIST_VNODE
#define CONF_DEFAULT_TIMEOUT                 5000
#define CONF_DEFAULT_LISTEN_BACKLOG          512
#define CONF_DEFAULT_CLIENT_CONNECTIONS      0
#define CONF_DEFAULT_DATASTORE				 DATA_REDIS
#define CONF_DEFAULT_PRECONNECT              true
#define CONF_DEFAULT_AUTO_EJECT_HOSTS        true
#define CONF_DEFAULT_SERVER_RETRY_TIMEOUT    10 * 1000      /* in msec */
#define CONF_DEFAULT_SERVER_FAILURE_LIMIT    2
#define CONF_DEFAULT_SERVER_CONNECTIONS      1
#define CONF_DEFAULT_KETAMA_PORT             11211

#define CONF_DEFAULT_SEEDS                   5
#define CONF_DEFAULT_DYN_READ_TIMEOUT        10000
#define CONF_DEFAULT_DYN_WRITE_TIMEOUT       10000
#define CONF_DEFAULT_DYN_CONNECTIONS         100
#define CONF_DEFAULT_VNODE_TOKENS            1
#define CONF_DEFAULT_GOS_INTERVAL            30000  //in millisec

#define CONF_STR_NONE                        "none"
#define CONF_STR_DC                          "datacenter"
#define CONF_STR_RACK                        "rack"
#define CONF_STR_ALL                         "all"

#define CONF_STR_DC_QUORUM                   "dc_quorum"
#define CONF_STR_DC_ONE                      "dc_one"


#define CONF_DEFAULT_RACK                    "localrack"
#define CONF_DEFAULT_DC                      "localdc"
#define CONF_DEFAULT_SECURE_SERVER_OPTION    CONF_STR_NONE

#define CONF_DEFAULT_SEED_PROVIDER           "simple_provider"

#define PEM_KEY_FILE  "conf/dynomite.pem"

data_store_t g_data_store = CONF_DEFAULT_DATASTORE;
struct command {
    struct string name;
    char          *(*set)(struct conf *cf, struct command *cmd, void *data);
    int           offset;
};

static rstatus_t
conf_server_init(struct conf_server *cs)
{
    string_init(&cs->pname);
    string_init(&cs->name);
    string_init(&cs->rack);
    string_init(&cs->dc);

    rstatus_t status = array_init(&cs->tokens, CONF_DEFAULT_VNODE_TOKENS,
                        sizeof(struct dyn_token));
    if (status != DN_OK) {
        string_deinit(&cs->pname);
        string_deinit(&cs->name);
        string_deinit(&cs->rack);
        string_deinit(&cs->dc);
        return status;
    }

    cs->port = 0;
    cs->weight = 0;

    memset(&cs->info, 0, sizeof(cs->info));

    cs->valid = 0;
    cs->is_secure = 0;

    log_debug(LOG_VVERB, "init conf server %p", cs);
    return DN_OK;
}

static void
conf_server_deinit(struct conf_server *cs)
{
    string_deinit(&cs->pname);
    string_deinit(&cs->name);
    string_deinit(&cs->rack);
    string_deinit(&cs->dc);
    array_deinit(&cs->tokens);
    cs->valid = 0;
    log_debug(LOG_VVERB, "deinit conf server %p", cs);
}

// copy from struct conf_server to struct datastore
static rstatus_t
conf_datastore_transform(struct datastore *s, struct conf_server *cs)
{
    ASSERT(cs->valid);
    ASSERT(s != NULL);
    s->owner = NULL;
    s->endpoint.pname = cs->pname;
    s->name = cs->name;
    s->endpoint.port = (uint16_t)cs->port;
    s->endpoint.weight = (uint32_t)cs->weight;

    s->endpoint.family = cs->info.family;
    s->endpoint.addrlen = cs->info.addrlen;
    s->endpoint.addr = (struct sockaddr *)&cs->info.addr;

    s->next_retry = 0ULL;
    s->failure_count = 0;

    log_debug(LOG_VERB, "transform to server '%.*s'",
              s->endpoint.pname.len, s->endpoint.pname.data);

    return DN_OK;
}

// Copy seed struct conf_server to struct server
rstatus_t
conf_seed_each_transform(void *elem, void *data)
{
    struct conf_server *cseed = elem;
    struct array *seeds = data;
    struct peer *s;

    ASSERT(cseed->valid);

    s = array_push(seeds);
    ASSERT(s != NULL);

    s->idx = array_idx(seeds, s);
    s->owner = NULL;
    s->endpoint.pname = cseed->pname;

    s->state = NORMAL;//assume peers are normal initially

    uint8_t *p = cseed->name.data + cseed->name.len - 1;
    uint8_t *start = cseed->name.data;
    string_copy(&s->name, start, dn_strrchr(p, start, ':') - start);

    s->rack = cseed->rack;
    s->dc = cseed->dc;
    //string_copy(&s->dc, cseed->dc.data, cseed->dc.len);

    s->is_local = false;
    //TODO-jeb need to copy over tokens, not sure if this is good enough
    s->tokens = cseed->tokens;

    s->endpoint.port = (uint16_t)cseed->port;
    s->endpoint.weight = (uint32_t)cseed->weight;
    s->endpoint.family = cseed->info.family;
    s->endpoint.addrlen = cseed->info.addrlen;
    s->endpoint.addr = (struct sockaddr *)&cseed->info.addr;
    s->conn = NULL;

    s->next_retry = 0ULL;
    s->failure_count = 0;

    s->processed = 0;
    s->is_seed = 1;
    s->is_secure = cseed->is_secure;

    log_debug(LOG_VERB, "transform to seed peer %"PRIu32" '%.*s'",
              s->idx, s->endpoint.pname.len, s->endpoint.pname.data);

    return DN_OK;
}

//TODOs: make sure to do a mem release for all these
static rstatus_t
conf_pool_init(struct conf_pool *cp, struct string *name)
{
    rstatus_t status;
    memset(cp, 0, sizeof(*cp));

    string_init(&cp->name);

    string_init(&cp->listen.pname);
    string_init(&cp->listen.name);

    string_init(&cp->rack);

    cp->listen.port = 0;
    memset(&cp->listen.info, 0, sizeof(cp->listen.info));
    cp->listen.valid = 0;

    cp->hash = CONF_UNSET_HASH;
    string_init(&cp->hash_tag);
    cp->distribution = CONF_UNSET_DIST;

    cp->timeout = CONF_UNSET_NUM;
    cp->backlog = CONF_UNSET_NUM;

    cp->client_connections = CONF_UNSET_NUM;

    cp->data_store = CONF_UNSET_NUM;
    cp->preconnect = CONF_UNSET_NUM;
    cp->auto_eject_hosts = CONF_UNSET_NUM;
    cp->server_connections = CONF_UNSET_NUM;
    cp->server_retry_timeout_ms = CONF_UNSET_NUM;
    cp->server_failure_limit = CONF_UNSET_NUM;

    //initialization for dynomite
    string_init(&cp->dyn_seed_provider);
    string_init(&cp->dyn_listen.pname);
    string_init(&cp->dyn_listen.name);
    string_init(&cp->secure_server_option);
    string_init(&cp->read_consistency);
    string_init(&cp->write_consistency);
    string_init(&cp->pem_key_file);
    string_init(&cp->dc);
    string_init(&cp->env);
    cp->dyn_listen.port = 0;
    memset(&cp->dyn_listen.info, 0, sizeof(cp->dyn_listen.info));
    cp->dyn_listen.valid = 0;

    cp->dyn_read_timeout = CONF_UNSET_NUM;
    cp->dyn_write_timeout = CONF_UNSET_NUM;
    cp->dyn_port = CONF_UNSET_NUM;
    cp->dyn_connections = CONF_UNSET_NUM;

    cp->gos_interval = CONF_UNSET_NUM;

    cp->conn_msg_rate = CONF_UNSET_NUM;

    array_null(&cp->dyn_seeds);

    cp->valid = 0;

    status = string_duplicate(&cp->name, name);
    if (status != DN_OK) {
        return status;
    }

    cp->conf_datastore = NULL;

    status = array_init(&cp->dyn_seeds, CONF_DEFAULT_SEEDS,
                        sizeof(struct conf_server));
    if (status != DN_OK) {
        string_deinit(&cp->name);
        return status;
    }

    status = array_init(&cp->tokens, CONF_DEFAULT_VNODE_TOKENS,
                        sizeof(struct dyn_token));
    if (status != DN_OK) {
        string_deinit(&cp->name);
        array_deinit(&cp->dyn_seeds);
        return status;
    }

    log_debug(LOG_VVERB, "init conf pool %p, '%.*s'", cp, name->len, name->data);

    return DN_OK;
}

static void
conf_pool_deinit(struct conf_pool *cp)
{
    string_deinit(&cp->name);

    string_deinit(&cp->listen.pname);
    string_deinit(&cp->listen.name);

    conf_server_deinit(cp->conf_datastore);
    dn_free(cp->conf_datastore);
    cp->conf_datastore = NULL;

    //deinit dynomite
    string_deinit(&cp->dyn_seed_provider);
    string_deinit(&cp->dyn_listen.pname);
    string_deinit(&cp->dyn_listen.name);
    string_deinit(&cp->secure_server_option);
    string_deinit(&cp->read_consistency);
    string_deinit(&cp->write_consistency);
    string_deinit(&cp->pem_key_file);
    string_deinit(&cp->dc);
    string_deinit(&cp->env);

    if (array_n(&cp->dyn_seeds) != 0)
       array_deinit(&cp->dyn_seeds);

    array_deinit(&cp->tokens);

    log_debug(LOG_VVERB, "deinit conf pool %p", cp);
}

static secure_server_option_t
get_secure_server_option(struct string option)
{
    if (dn_strcmp(option.data, CONF_STR_NONE) == 0) {
        return SECURE_OPTION_NONE;
    }
    if (dn_strcmp(option.data, CONF_STR_RACK) == 0) {
        return SECURE_OPTION_RACK;
    }
    if (dn_strcmp(option.data, CONF_STR_DC) == 0) {
        return SECURE_OPTION_DC;
    }
    if (dn_strcmp(option.data, CONF_STR_ALL) == 0) {
        return SECURE_OPTION_ALL;
    }
    return SECURE_OPTION_NONE;
}

static rstatus_t
conf_topo_transform(struct topology *topo, struct conf_pool *cp)
{
    string_duplicate(&topo->seed_provider, &cp->dyn_seed_provider);
    topo->key_hash_type = cp->hash;
    topo->key_hash = hash_algos[cp->hash];
    topo->dist_type = cp->distribution;
    topo->hash_tag = cp->hash_tag;

    return DN_OK;
}

rstatus_t
conf_pool_transform(struct server_pool *sp, struct conf_pool *cp)
{
    rstatus_t status;
    ASSERT(cp->valid);

    memset(sp, 0, sizeof(struct server_pool));

    sp->ctx = NULL;
    sp->p_conn = NULL;
    sp->dn_conn_q = 0;
    TAILQ_INIT(&sp->c_conn_q);

    /* sp->ncontinuum = 0; */
    /* sp->nserver_continuum = 0; */
    /* sp->continuum = NULL; */
    sp->nlive_server = 0;
    sp->next_rebuild = 0ULL;

    sp->name = cp->name;
    sp->proxy_endpoint.pname = cp->listen.pname;
    sp->proxy_endpoint.port = (uint16_t)cp->listen.port;

    sp->proxy_endpoint.family = cp->listen.info.family;
    sp->proxy_endpoint.addrlen = cp->listen.info.addrlen;
    sp->proxy_endpoint.addr = (struct sockaddr *)&cp->listen.info.addr;

    g_data_store = cp->data_store;
    if ((g_data_store != DATA_REDIS) &&
        (g_data_store != DATA_MEMCACHE)) {
        log_error("Invalid datastore in conf file");
        return DN_ERROR;
    }
    set_datastore_ops();
    sp->timeout = cp->timeout;
    sp->backlog = cp->backlog;

    sp->client_connections = (uint32_t)cp->client_connections;

    sp->server_connections = (uint32_t)cp->server_connections;
    sp->server_retry_timeout_ms = cp->server_retry_timeout_ms;
    sp->server_failure_limit = (uint32_t)cp->server_failure_limit;
    sp->auto_eject_hosts = cp->auto_eject_hosts ? 1 : 0;
    sp->preconnect = cp->preconnect ? 1 : 0;

    sp->datastore = dn_zalloc(sizeof(*sp->datastore));
    status = conf_datastore_transform(sp->datastore, cp->conf_datastore);
    if (status != DN_OK) {
        return status;
    }
    sp->datastore->owner = sp;
	log_debug(LOG_DEBUG, "init datastore in pool '%.*s'",
			  sp->name.len, sp->name.data);

    /* dynomite init */
    sp->dnode_proxy_endpoint.pname = cp->dyn_listen.pname;
    sp->dnode_proxy_endpoint.port = (uint16_t)cp->dyn_listen.port;
    sp->dnode_proxy_endpoint.family = cp->dyn_listen.info.family;
    sp->dnode_proxy_endpoint.addrlen = cp->dyn_listen.info.addrlen;
    sp->dnode_proxy_endpoint.addr = (struct sockaddr *)&cp->dyn_listen.info.addr;
    sp->peer_connections = (uint32_t)cp->dyn_connections;
    sp->rack_name = cp->rack;
    sp->dc_name = cp->dc;
    sp->tokens = cp->tokens;
    sp->env = cp->env;

    sp->secure_server_option = get_secure_server_option(cp->secure_server_option);
    sp->pem_key_file = cp->pem_key_file;

    sp->conf_pool = cp;

    /* gossip */
    sp->g_interval = cp->gos_interval;

    set_msgs_per_sec(cp->conn_msg_rate);
    sp->topo = topo_create();
    conf_topo_transform(sp->topo, cp);

    log_debug(LOG_VERB, "transform to pool '%.*s'", sp->name.len, sp->name.data);

    return DN_OK;
}

static void
conf_dump(struct conf *cf)
{
    uint32_t j;
    struct string *s;

    log_debug(LOG_VVERB, "pool in configuration file '%s'", cf->fname);

    struct conf_pool *cp = &cf->pool;

    log_debug(LOG_VVERB, "%.*s", cp->name.len, cp->name.data);
    log_debug(LOG_VVERB, "  listen: %.*s",
            cp->listen.pname.len, cp->listen.pname.data);
    log_debug(LOG_VVERB, "  timeout: %d", cp->timeout);
    log_debug(LOG_VVERB, "  backlog: %d", cp->backlog);
    log_debug(LOG_VVERB, "  hash: %d", cp->hash);
    log_debug(LOG_VVERB, "  hash_tag: \"%.*s\"", cp->hash_tag.len,
            cp->hash_tag.data);
    log_debug(LOG_VVERB, "  distribution: %d", cp->distribution);
    log_debug(LOG_VVERB, "  client_connections: %d",
            cp->client_connections);
    const char * temp_log = "unknown";
    if(g_data_store == DATA_REDIS){
        temp_log = "redis";
    }
    else if(g_data_store == DATA_MEMCACHE){
        temp_log = "memcache";
    }
    log_debug(LOG_VVERB, "  data_store: %d (%s)", g_data_store, temp_log);
    log_debug(LOG_VVERB, "  preconnect: %d", cp->preconnect);
    log_debug(LOG_VVERB, "  auto_eject_hosts: %d", cp->auto_eject_hosts);
    log_debug(LOG_VVERB, "  server_connections: %d",
            cp->server_connections);
    log_debug(LOG_VVERB, "  server_retry_timeout: %d (msec)",
            cp->server_retry_timeout_ms);
    log_debug(LOG_VVERB, "  server_failure_limit: %d",
            cp->server_failure_limit);

    log_debug(LOG_VVERB, "  datastore: ");
    log_debug(LOG_VVERB, "    %.*s",
            cp->conf_datastore->name.len, cp->conf_datastore->name.data);

    log_debug(LOG_VVERB, "  dyn_seed_provider: \"%.*s\"", cp->dyn_seed_provider.len, cp->dyn_seed_provider.data);

    uint32_t nseeds = array_n(&cp->dyn_seeds);
    log_debug(LOG_VVERB, "  dyn_seeds: %"PRIu32"", nseeds);
    for (j = 0; j < nseeds; j++) {
        s = array_get(&cp->dyn_seeds, j);
        log_debug(LOG_VVERB, "    %.*s", s->len, s->data);
    }

    log_debug(LOG_VVERB, "  env: %.*s", cp->env.len, cp->env.data);
    log_debug(LOG_VVERB, "  rack: %.*s", cp->rack.len, cp->rack.data);
    log_debug(LOG_VVERB, "  dc: %.*s", cp->dc.len, cp->dc.data);

    log_debug(LOG_VVERB, "  dyn_listen: %.*s",
            cp->dyn_listen.pname.len, cp->dyn_listen.pname.data);
    log_debug(LOG_VVERB, "  dyn_read_timeout: %d", cp->dyn_read_timeout);
    log_debug(LOG_VVERB, "  dyn_write_timeout: %d", cp->dyn_write_timeout);
    log_debug(LOG_VVERB, "  dyn_connections: %d", cp->dyn_connections);

    log_debug(LOG_VVERB, "  gos_interval: %d", cp->gos_interval);
    log_debug(LOG_VVERB, "  conn_msg_rate: %d", cp->conn_msg_rate);

    log_debug(LOG_VVERB, "  secure_server_option: \"%.*s\"",
            cp->secure_server_option.len,
            cp->secure_server_option.data);

    log_debug(LOG_VVERB, "  read_consistency: \"%.*s\"",
            cp->read_consistency.len,
            cp->read_consistency.data);

    log_debug(LOG_VVERB, "  write_consistency: \"%.*s\"",
            cp->write_consistency.len,
            cp->write_consistency.data);

    log_debug(LOG_VVERB, "  dc: \"%.*s\"", cp->dc.len, cp->dc.data);
}

static rstatus_t
conf_yaml_init(struct conf *cf)
{
    int rv;

    ASSERT(!cf->valid_parser);

    rv = fseek(cf->fh, 0L, SEEK_SET);
    if (rv < 0) {
        log_error("conf: failed to seek to the beginning of file '%s': %s",
                  cf->fname, strerror(errno));
        return DN_ERROR;
    }

    rv = yaml_parser_initialize(&cf->parser);
    if (!rv) {
        log_error("conf: failed (err %d) to initialize yaml parser",
                  cf->parser.error);
        return DN_ERROR;
    }

    yaml_parser_set_input_file(&cf->parser, cf->fh);
    cf->valid_parser = 1;

    return DN_OK;
}

static void
conf_yaml_deinit(struct conf *cf)
{
    if (cf->valid_parser) {
        yaml_parser_delete(&cf->parser);
        cf->valid_parser = 0;
    }
}

static rstatus_t
conf_token_next(struct conf *cf)
{
    int rv;

    ASSERT(cf->valid_parser && !cf->valid_token);

    rv = yaml_parser_scan(&cf->parser, &cf->token);
    if (!rv) {
        log_error("conf: failed (err %d) to scan next token", cf->parser.error);
        return DN_ERROR;
    }
    cf->valid_token = 1;

    return DN_OK;
}

static void
conf_token_done(struct conf *cf)
{
    ASSERT(cf->valid_parser);

    if (cf->valid_token) {
        yaml_token_delete(&cf->token);
        cf->valid_token = 0;
    }
}

static rstatus_t
conf_event_next(struct conf *cf)
{
    int rv;

    ASSERT(cf->valid_parser && !cf->valid_event);

    rv = yaml_parser_parse(&cf->parser, &cf->event);
    if (!rv) {
        log_error("conf: failed (err %d) to get next event", cf->parser.error);
        return DN_ERROR;
    }
    cf->valid_event = 1;

    return DN_OK;
}

static void
conf_event_done(struct conf *cf)
{
    if (cf->valid_event) {
        yaml_event_delete(&cf->event);
        cf->valid_event = 0;
    }
}

static rstatus_t
conf_push_scalar(struct conf *cf)
{
    rstatus_t status;
    struct string *value;
    uint8_t *scalar;
    uint32_t scalar_len;

    scalar = cf->event.data.scalar.value;
    scalar_len = (uint32_t)cf->event.data.scalar.length;
    if (scalar_len == 0) {
    	return DN_ERROR;
    }
    log_debug(LOG_VVERB, "push '%.*s'", scalar_len, scalar);

    value = array_push(&cf->arg);
    if (value == NULL) {
        return DN_ENOMEM;
    }
    string_init(value);

    status = string_copy(value, scalar, scalar_len);
    if (status != DN_OK) {
        array_pop(&cf->arg);
        return status;
    }

    return DN_OK;
}

static void
conf_pop_scalar(struct conf *cf)
{
    struct string *value;

    value = array_pop(&cf->arg);
    log_debug(LOG_VVERB, "pop '%.*s'", value->len, value->data);
    string_deinit(value);
}

static char *
conf_set_string(struct conf *cf, struct command *cmd, void *conf)
{
    rstatus_t status;
    uint8_t *p;
    struct string *field, *value;

    p = conf;
    field = (struct string *)(p + cmd->offset);

    if (field->data != CONF_UNSET_PTR) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);

    status = string_duplicate(field, value);
    if (status != DN_OK) {
        return CONF_ERROR;
    }

    return CONF_OK;
}

static char *
conf_set_listen(struct conf *cf, struct command *cmd, void *conf)
{
    rstatus_t status;
    struct string *value;
    struct conf_listen *field;
    uint8_t *p, *name;
    uint32_t namelen;

    p = conf;
    field = (struct conf_listen *)(p + cmd->offset);

    if (field->valid == 1) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);

    status = string_duplicate(&field->pname, value);
    if (status != DN_OK) {
        return CONF_ERROR;
    }

    if (value->data[0] == '/') {
        name = value->data;
        namelen = value->len;
    } else {
        uint8_t *q, *start, *port;
        uint32_t portlen;

        /* parse "hostname:port" from the end */
        p = value->data + value->len - 1;
        start = value->data;
        q = dn_strrchr(p, start, ':');
        if (q == NULL) {
            return "has an invalid \"hostname:port\" format string";
        }

        port = q + 1;
        portlen = (uint32_t)(p - port + 1);

        p = q - 1;

        name = start;
        namelen = (uint32_t)(p - start + 1);

        field->port = dn_atoi(port, portlen);
        if (field->port < 0 || !dn_valid_port(field->port)) {
            return "has an invalid port in \"hostname:port\" format string";
        }
    }

    status = string_copy(&field->name, name, namelen);
    if (status != DN_OK) {
        return CONF_ERROR;
    }

    status = dn_resolve(&field->name, field->port, &field->info);
    if (status != DN_OK) {
        return CONF_ERROR;
    }

    field->valid = 1;

    return CONF_OK;
}

/* Parses servers: from yaml */
static char *
conf_add_server(struct conf *cf, struct command *cmd, void *conf)
{
    rstatus_t status;
    struct string *value;
    struct conf_server *field;
    uint8_t *p, *q, *start;
    uint8_t *pname, *addr, *port, *weight, *name;
    uint32_t k, delimlen, pnamelen, addrlen, portlen, weightlen, namelen;
    struct string address;
    char delim[] = " ::";

    string_init(&address);
    p = conf;
    struct conf_server **pfield = (struct conf_server **)(p + cmd->offset);
    ASSERT(*pfield == NULL);
    *pfield = (struct conf_server *)dn_zalloc(sizeof(struct conf_server));
    field = *pfield;
    status = conf_server_init(field);
    if (status != DN_OK) {
        dn_free(*pfield);
        *pfield = NULL;
        return CONF_ERROR;
    }

    value = array_top(&cf->arg);

    /* parse "hostname:port:weight [name]" or "/path/unix_socket:weight [name]" from the end */
    p = value->data + value->len - 1;
    start = value->data;
    addr = NULL;
    addrlen = 0;
    weight = NULL;
    weightlen = 0;
    port = NULL;
    portlen = 0;
    name = NULL;
    namelen = 0;

    delimlen = value->data[0] == '/' ? 2 : 3;

    for (k = 0; k < sizeof(delim); k++) {
        q = dn_strrchr(p, start, delim[k]);
        if (q == NULL) {
            if (k == 0) {
                /*
                 * name in "hostname:port:weight [name]" format string is
                 * optional
                 */
                continue;
            }
            break;
        }

        switch (k) {
        case 0:
            name = q + 1;
            namelen = (uint32_t)(p - name + 1);
            break;

        case 1:
            weight = q + 1;
            weightlen = (uint32_t)(p - weight + 1);
            break;

        case 2:
            port = q + 1;
            portlen = (uint32_t)(p - port + 1);
            break;

        default:
            NOT_REACHED();
        }

        p = q - 1;
    }

    if (k != delimlen) {
        return "has an invalid \"hostname:port:weight [name]\"or \"/path/unix_socket:weight [name]\" format string";
    }

    pname = value->data;
    pnamelen = namelen > 0 ? value->len - (namelen + 1) : value->len;
    status = string_copy(&field->pname, pname, pnamelen);
    if (status != DN_OK) {
        dn_free(*pfield);
        *pfield = NULL;
        return CONF_ERROR;
    }

    addr = start;
    addrlen = (uint32_t)(p - start + 1);

    field->weight = dn_atoi(weight, weightlen);
    if (field->weight < 0) {
        return "has an invalid weight in \"hostname:port:weight [name]\" format string";
    }

    if (value->data[0] != '/') {
        field->port = dn_atoi(port, portlen);
        if (field->port < 0 || !dn_valid_port(field->port)) {
            return "has an invalid port in \"hostname:port:weight [name]\" format string";
        }
    }

    if (name == NULL) {
        /*
         * To maintain backward compatibility with libmemcached, we don't
         * include the port as the part of the input string to the consistent
         * hashing algorithm, when it is equal to 11211.
         */
        if (field->port == CONF_DEFAULT_KETAMA_PORT) {
            name = addr;
            namelen = addrlen;
        } else {
            name = addr;
            namelen = addrlen + 1 + portlen;
        }
    }

    status = string_copy(&field->name, name, namelen);
    if (status != DN_OK) {
        return CONF_ERROR;
    }

    status = string_copy(&address, addr, addrlen);
    if (status != DN_OK) {
        return CONF_ERROR;
    }


    status = dn_resolve(&address, field->port, &field->info);
    if (status != DN_OK) {
        string_deinit(&address);
        return CONF_ERROR;
    }

    string_deinit(&address);
    field->valid = 1;

    return CONF_OK;
}


/*
 * Well, this just blows. Copied from conf_add_server() there is a colon delimited
 * string in the yaml that requires a few levels of magic in order to guess the
 * structure of, rather than doing the right thing and making proper fields.
 * Need to fix however, there's bigger fish to deep fry at this point.
 */
static char *
conf_add_dyn_server(struct conf *cf, struct command *cmd, void *conf)
{
    rstatus_t status;
    struct array *a;
    struct string *value;
    struct conf_server *field;
    uint8_t *p, *q, *start;
    uint8_t *pname, *addr, *port, *rack, *tokens, *name, *dc;
    uint32_t k, delimlen, pnamelen, addrlen, portlen, racklen, tokenslen, namelen, dclen;
    struct string address;
    char delim[] = " ::::";

    string_init(&address);
    p = conf; // conf_pool
    a = (struct array *)(p + cmd->offset); // a is conf_server array

    field = array_push(a);
    if (field == NULL) {
        return CONF_ERROR;
    }

    status = conf_server_init(field); // field is conf_server
    if (status != DN_OK) {
        return CONF_ERROR;
    }

    value = array_top(&cf->arg);

    /* parse "hostname:port:rack:dc:tokens [name]" */
    p = value->data + value->len - 1; // p is now pointing to a string
    start = value->data;
    addr = NULL;
    addrlen = 0;
    rack = NULL;
    racklen = 0;
    tokens = NULL;
    tokenslen = 0;
    port = NULL;
    portlen = 0;
    name = NULL;
    namelen = 0;
    dc = NULL;
    dclen = 0;

    delimlen = 5;

    for (k = 0; k < sizeof(delim); k++) {
        q = dn_strrchr(p, start, delim[k]);
        if (q == NULL) {
            if (k == 0) {
                /*
                 * name in "hostname:port:rack:dc:tokens [name]" format string is
                 * optional
                 */
                continue;
            }
            break;
        }

        switch (k) {
        case 0:
            name = q + 1;
            namelen = (uint32_t)(p - name + 1);
            break;

        case 1:
            tokens = q + 1;
            tokenslen = (uint32_t)(p - tokens + 1);
            break;

        case 2:
            dc = q + 1;
            dclen = (uint32_t)(p - dc + 1);
            break;

        case 3:
            rack = q + 1;
            racklen = (uint32_t)(p - rack + 1);
            break;

        case 4:
            port = q + 1;
            portlen = (uint32_t)(p - port + 1);
            break;

        default:
            NOT_REACHED();
        }

        p = q - 1;
    }

    if (k != delimlen) {
        return "has an invalid format must match \"hostname:port:rack:dc:tokens [name]\"";
    }

    pname = value->data; // seed node config string.
    pnamelen = namelen > 0 ? value->len - (namelen + 1) : value->len;
    status = string_copy(&field->pname, pname, pnamelen);
    if (status != DN_OK) {
        array_pop(a);
        return CONF_ERROR;
    }

    status = string_copy(&field->dc, dc, dclen);
    if (status != DN_OK) {
        array_pop(a);
        return CONF_ERROR;
    }

    status = string_copy(&field->rack, rack, racklen);
    if (status != DN_OK) {
        array_pop(a);
        return CONF_ERROR;
    }

    uint8_t *t_end = tokens + tokenslen;
    status = derive_tokens(&field->tokens, tokens, t_end);
    if (status != DN_OK) {
        array_pop(a);
        return CONF_ERROR;
    }

    addr = start;
    addrlen = (uint32_t)(p - start + 1);
    if (value->data[0] != '/') {
        field->port = dn_atoi(port, portlen);
        if (field->port < 0 || !dn_valid_port(field->port)) {
            return "has an invalid port in \"hostname:port:weight [name]\" format string";
        }
    }

    if (name == NULL) {
        /*
         * To maintain backward compatibility with libmemcached, we don't
         * include the port as the part of the input string to the consistent
         * hashing algorithm, when it is equal to 11211.
         */
        if (field->port == CONF_DEFAULT_KETAMA_PORT) {
            name = addr;
            namelen = addrlen;
        } else {
            name = addr;
            namelen = addrlen + 1 + portlen;
        }
    }

    status = string_copy(&field->name, name, namelen);
    if (status != DN_OK) {
        return CONF_ERROR;
    }

    status = string_copy(&address, addr, addrlen);
    if (status != DN_OK) {
        return CONF_ERROR;
    }

    status = dn_resolve(&address, field->port, &field->info);
    if (status != DN_OK) {
        string_deinit(&address);
        return CONF_ERROR;
    }

    string_deinit(&address);
    field->valid = 1;

    return CONF_OK;
}

static char *
conf_set_tokens(struct conf *cf, struct command *cmd, void *conf)
{
    uint8_t *p = conf;
    struct array *tokens = (struct array *)(p + cmd->offset);
    struct string *value = array_top(&cf->arg);
    p = value->data + value->len;

    rstatus_t status = derive_tokens(tokens, value->data, p);
    if (status != DN_OK) {
        //TODO: should we dealloc the tokens/array?
        return CONF_ERROR;
    }

    return CONF_OK;
}

static char *
conf_set_num(struct conf *cf, struct command *cmd, void *conf)
{
    uint8_t *p;
    int num, *np;
    struct string *value;

    p = conf;
    np = (int *)(p + cmd->offset);

    if (*np != CONF_UNSET_NUM) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);

    num = dn_atoi(value->data, value->len);
    if (num < 0) {
        return "is not a number";
    }

    *np = num;

    return CONF_OK;
}

static char *
conf_set_bool(struct conf *cf, struct command *cmd, void *conf)
{
    uint8_t *p;
    int *bp;
    struct string *value, true_str, false_str;

    p = conf;
    bp = (int *)(p + cmd->offset);

    if (*bp != CONF_UNSET_NUM) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);
    string_set_text(&true_str, "true");
    string_set_text(&false_str, "false");

    if (string_compare(value, &true_str) == 0) {
        *bp = 1;
    } else if (string_compare(value, &false_str) == 0) {
        *bp = 0;
    } else {
        return "is not \"true\" or \"false\"";
    }

    return CONF_OK;
}

static char *
conf_set_hash(struct conf *cf, struct command *cmd, void *conf)
{
    uint8_t *p;
    hash_type_t *hp;
    struct string *value, *hash;

    p = conf;
    hp = (hash_type_t *)(p + cmd->offset);

    if (*hp != CONF_UNSET_HASH) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);

    for (hash = hash_strings; hash->len != 0; hash++) {
        if (string_compare(value, hash) != 0) {
            continue;
        }

        *hp = hash - hash_strings;

        return CONF_OK;
    }

    return "is not a valid hash";
}

static char *
conf_set_distribution(struct conf *cf, struct command *cmd, void *conf)
{
    uint8_t *p;
    dist_type_t *dp;
    struct string *value, *dist;

    p = conf;
    dp = (dist_type_t *)(p + cmd->offset);

    if (*dp != CONF_UNSET_DIST) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);

    for (dist = dist_strings; dist->len != 0; dist++) {
        if (string_compare(value, dist) != 0) {
            continue;
        }

        *dp = dist - dist_strings;

        return CONF_OK;
    }

    return "is not a valid distribution";
}

static char *
conf_set_hashtag(struct conf *cf, struct command *cmd, void *conf)
{
    rstatus_t status;
    uint8_t *p;
    struct string *field, *value;

    p = conf;
    field = (struct string *)(p + cmd->offset);

    if (field->data != CONF_UNSET_PTR) {
        return "is a duplicate";
    }

    value = array_top(&cf->arg);

    if (value->len != 2) {
        return "is not a valid hash tag string with two characters";
    }

    status = string_duplicate(field, value);
    if (status != DN_OK) {
        return CONF_ERROR;
    }

    return CONF_OK;
}
static struct command conf_commands[] = {
    { string("listen"),
      conf_set_listen,
      offsetof(struct conf_pool, listen) },

    { string("hash"),
      conf_set_hash,
      offsetof(struct conf_pool, hash) },

    { string("hash_tag"),
      conf_set_hashtag,
      offsetof(struct conf_pool, hash_tag) },

    { string("distribution"),
      conf_set_distribution,
      offsetof(struct conf_pool, distribution) },

    { string("timeout"),
      conf_set_num,
      offsetof(struct conf_pool, timeout) },

    { string("backlog"),
      conf_set_num,
      offsetof(struct conf_pool, backlog) },

    { string("client_connections"),
      conf_set_num,
      offsetof(struct conf_pool, client_connections) },

    { string("data_store"),
      conf_set_num,
      offsetof(struct conf_pool, data_store) },

    { string("preconnect"),
      conf_set_bool,
      offsetof(struct conf_pool, preconnect) },

    { string("auto_eject_hosts"),
      conf_set_bool,
      offsetof(struct conf_pool, auto_eject_hosts) },

    { string("server_connections"),
      conf_set_num,
      offsetof(struct conf_pool, server_connections) },

    { string("server_retry_timeout"),
      conf_set_num,
      offsetof(struct conf_pool, server_retry_timeout_ms) },

    { string("server_failure_limit"),
      conf_set_num,
      offsetof(struct conf_pool, server_failure_limit) },

    { string("servers"),
      conf_add_server,
      offsetof(struct conf_pool, conf_datastore) },

    { string("dyn_read_timeout"),
      conf_set_num,
      offsetof(struct conf_pool, dyn_read_timeout) },

    { string("dyn_write_timeout"),
      conf_set_num,
      offsetof(struct conf_pool, dyn_write_timeout) },

    { string("dyn_listen"),
      conf_set_listen,
      offsetof(struct conf_pool, dyn_listen) },

    { string("dyn_seed_provider"),
      conf_set_string,
      offsetof(struct conf_pool, dyn_seed_provider) },

    { string("dyn_seeds"),
      conf_add_dyn_server,
      offsetof(struct conf_pool, dyn_seeds) },

    { string("dyn_port"),
      conf_set_num,
      offsetof(struct conf_pool, dyn_port) },

    { string("dyn_connections"),
      conf_set_num,
      offsetof(struct conf_pool, dyn_connections) },

    { string("rack"),
      conf_set_string,
      offsetof(struct conf_pool, rack) },

    { string("tokens"),
      conf_set_tokens,
      offsetof(struct conf_pool, tokens) },

    { string("gos_interval"),
      conf_set_num,
      offsetof(struct conf_pool, gos_interval) },

    { string("secure_server_option"),
      conf_set_string,
      offsetof(struct conf_pool, secure_server_option) },

    { string("pem_key_file"),
        conf_set_string,
        offsetof(struct conf_pool, pem_key_file) },

    { string("datacenter"),
      conf_set_string,
      offsetof(struct conf_pool, dc) },

    { string("env"),
      conf_set_string,
      offsetof(struct conf_pool, env) },

    { string("conn_msg_rate"),
      conf_set_num,
      offsetof(struct conf_pool, conn_msg_rate)},

    { string("read_consistency"),
      conf_set_string,
      offsetof(struct conf_pool, read_consistency) },

    { string("write_consistency"),
      conf_set_string,
      offsetof(struct conf_pool, write_consistency) },

    null_command
};

static rstatus_t
conf_handler(struct conf *cf, void *data)
{
    struct command *cmd;
    struct string *key, *value;
    uint32_t narg;

    if (array_n(&cf->arg) == 1) {
        value = array_top(&cf->arg);
        log_debug(LOG_VVERB, "conf handler on '%.*s'", value->len, value->data);
        return conf_pool_init(data, value);
    }

    narg = array_n(&cf->arg);
    value = array_get(&cf->arg, narg - 1);
    key = array_get(&cf->arg, narg - 2);

    log_debug(LOG_VVERB, "conf handler on %.*s: %.*s", key->len, key->data,
              value->len, value->data);

    for (cmd = conf_commands; cmd->name.len != 0; cmd++) {
        char *rv;

        if (string_compare(key, &cmd->name) != 0) {
            continue;
        }

        rv = cmd->set(cf, cmd, data);
        if (rv != CONF_OK) {
            log_error("conf: directive \"%.*s\" %s", key->len, key->data, rv);
            return DN_ERROR;
        }

        return DN_OK;
    }

    log_error("conf: directive \"%.*s\" is unknown", key->len, key->data);

    return DN_ERROR;
}

static rstatus_t
conf_begin_parse(struct conf *cf)
{
    rstatus_t status;
    bool done;

    ASSERT(cf->sound && !cf->parsed);
    ASSERT(cf->depth == 0);

    status = conf_yaml_init(cf);
    if (status != DN_OK) {
        return status;
    }

    done = false;
    do {
        status = conf_event_next(cf);
        if (status != DN_OK) {
            return status;
        }

        log_debug(LOG_VVERB, "next begin event %d", cf->event.type);

        switch (cf->event.type) {
        case YAML_STREAM_START_EVENT:
        case YAML_DOCUMENT_START_EVENT:
            break;

        case YAML_MAPPING_START_EVENT:
            ASSERT(cf->depth < CONF_MAX_DEPTH);
            cf->depth++;
            done = true;
            break;

        default:
            NOT_REACHED();
        }

        conf_event_done(cf);

    } while (!done);

    return DN_OK;
}

static rstatus_t
conf_end_parse(struct conf *cf)
{
    rstatus_t status;
    bool done;

    ASSERT(cf->sound && !cf->parsed);
    ASSERT(cf->depth == 0);

    done = false;
    do {
        status = conf_event_next(cf);
        if (status != DN_OK) {
            return status;
        }

        log_debug(LOG_VVERB, "next end event %d", cf->event.type);

        switch (cf->event.type) {
        case YAML_STREAM_END_EVENT:
            done = true;
            break;

        case YAML_DOCUMENT_END_EVENT:
            break;

        default:
            NOT_REACHED();
        }

        conf_event_done(cf);
    } while (!done);

    conf_yaml_deinit(cf);

    return DN_OK;
}

static rstatus_t
conf_parse_core(struct conf *cf, void *data)
{
    rstatus_t status;
    bool done, leaf, new_pool;

    ASSERT(cf->sound);

    status = conf_event_next(cf);
    if (status != DN_OK) {
        return status;
    }

    log_debug(LOG_VVERB, "next event %d depth %"PRIu32" seq %d", cf->event.type,
              cf->depth, cf->seq);

    done = false;
    leaf = false;
    new_pool = false;

    switch (cf->event.type) {
    case YAML_MAPPING_END_EVENT:
        cf->depth--;
        if (cf->depth == 1) {
            conf_pop_scalar(cf);
        } else if (cf->depth == 0) {
            done = true;
        }
        break;

    case YAML_MAPPING_START_EVENT:
        cf->depth++;
        break;

    case YAML_SEQUENCE_START_EVENT:
        cf->seq = 1;
        break;

    case YAML_SEQUENCE_END_EVENT:
        conf_pop_scalar(cf);
        cf->seq = 0;
        break;

    case YAML_SCALAR_EVENT:
        status = conf_push_scalar(cf);
        if (status != DN_OK) {
            break;
        }

        /* take appropriate action */
        if (cf->seq) {
            /* for a sequence, leaf is at CONF_MAX_DEPTH */
            ASSERT(cf->depth == CONF_MAX_DEPTH);
            leaf = true;
        } else if (cf->depth == CONF_ROOT_DEPTH) {
           data = &cf->pool;
           new_pool = true;
        } else if (array_n(&cf->arg) == cf->depth + 1) {
            /* for {key: value}, leaf is at CONF_MAX_DEPTH */
            ASSERT(cf->depth == CONF_MAX_DEPTH);
            leaf = true;
        }
        break;

    default:
        NOT_REACHED();
        break;
    }

    conf_event_done(cf);

    if (status != DN_OK) {
        return status;
    }

    if (done) {
        /* terminating condition */
        return DN_OK;
    }

    if (leaf || new_pool) {
        status = conf_handler(cf, data);

        if (leaf) {
            conf_pop_scalar(cf);
            if (!cf->seq) {
                conf_pop_scalar(cf);
            }
        }

        if (status != DN_OK) {
            return status;
        }
    }

    return conf_parse_core(cf, data);
}

static rstatus_t
conf_parse(struct conf *cf)
{
    rstatus_t status;

    ASSERT(cf->sound && !cf->parsed);
    ASSERT(array_n(&cf->arg) == 0);

    status = conf_begin_parse(cf);
    if (status != DN_OK) {
        return status;
    }

    status = conf_parse_core(cf, NULL);
    if (status != DN_OK) {
        return status;
    }

    status = conf_end_parse(cf);
    if (status != DN_OK) {
        return status;
    }

    cf->parsed = 1;

    return DN_OK;
}

static struct conf *
conf_open(char *filename)
{
    rstatus_t status;
    struct conf *cf;
    FILE *fh;

    fh = fopen(filename, "r");
    if (fh == NULL) {
        log_error("conf: failed to open configuration '%s': %s", filename,
                  strerror(errno));
        return NULL;
    }

    cf = dn_zalloc(sizeof(*cf));
    if (cf == NULL) {
        fclose(fh);
        return NULL;
    }

    status = array_init(&cf->arg, CONF_DEFAULT_ARGS, sizeof(struct string));
    if (status != DN_OK) {
        dn_free(cf);
        fclose(fh);
        return NULL;
    }

    cf->fname = filename;
    cf->fh = fh;
    cf->depth = 0;
    /* parser, event, and token are initialized later */
    cf->seq = 0;
    cf->valid_parser = 0;
    cf->valid_event = 0;
    cf->valid_token = 0;
    cf->sound = 0;
    cf->parsed = 0;
    cf->valid = 0;

    log_debug(LOG_VVERB, "opened conf '%s'", filename);

    return cf;
}

static rstatus_t
conf_validate_document(struct conf *cf)
{
    rstatus_t status;
    uint32_t count;
    bool done;

    status = conf_yaml_init(cf);
    if (status != DN_OK) {
        return status;
    }

    count = 0;
    done = false;
    do {
        yaml_document_t document;
        yaml_node_t *node;
        int rv;

        rv = yaml_parser_load(&cf->parser, &document);
        if (!rv) {
            log_error("conf: failed (err %d) to get the next yaml document",
                      cf->parser.error);
            conf_yaml_deinit(cf);
            return DN_ERROR;
        }

        node = yaml_document_get_root_node(&document);
        if (node == NULL) {
            done = true;
        } else {
            count++;
        }

        yaml_document_delete(&document);
    } while (!done);

    conf_yaml_deinit(cf);

    if (count != 1) {
        log_error("conf: '%s' must contain only 1 document; found %"PRIu32" "
                  "documents", cf->fname, count);
        return DN_ERROR;
    }

    return DN_OK;
}

static rstatus_t
conf_validate_tokens(struct conf *cf)
{
    rstatus_t status;
    bool done, error;
    int type;

    status = conf_yaml_init(cf);
    if (status != DN_OK) {
        return status;
    }

    done = false;
    error = false;
    do {
        status = conf_token_next(cf);
        if (status != DN_OK) {
            return status;
        }
        type = cf->token.type;

        switch (type) {
        case YAML_NO_TOKEN:
            error = true;
            log_error("conf: no token (%d) is disallowed", type);
            break;

        case YAML_VERSION_DIRECTIVE_TOKEN:
            error = true;
            log_error("conf: version directive token (%d) is disallowed", type);
            break;

        case YAML_TAG_DIRECTIVE_TOKEN:
            error = true;
            log_error("conf: tag directive token (%d) is disallowed", type);
            break;

        case YAML_DOCUMENT_START_TOKEN:
            error = true;
            log_error("conf: document start token (%d) is disallowed", type);
            break;

        case YAML_DOCUMENT_END_TOKEN:
            error = true;
            log_error("conf: document end token (%d) is disallowed", type);
            break;

        case YAML_FLOW_SEQUENCE_START_TOKEN:
            error = true;
            log_error("conf: flow sequence start token (%d) is disallowed", type);
            break;

        case YAML_FLOW_SEQUENCE_END_TOKEN:
            error = true;
            log_error("conf: flow sequence end token (%d) is disallowed", type);
            break;

        case YAML_FLOW_MAPPING_START_TOKEN:
            error = true;
            log_error("conf: flow mapping start token (%d) is disallowed", type);
            break;

        case YAML_FLOW_MAPPING_END_TOKEN:
            error = true;
            log_error("conf: flow mapping end token (%d) is disallowed", type);
            break;

        case YAML_FLOW_ENTRY_TOKEN:
            error = true;
            log_error("conf: flow entry token (%d) is disallowed", type);
            break;

        case YAML_ALIAS_TOKEN:
            error = true;
            log_error("conf: alias token (%d) is disallowed", type);
            break;

        case YAML_ANCHOR_TOKEN:
            error = true;
            log_error("conf: anchor token (%d) is disallowed", type);
            break;

        case YAML_TAG_TOKEN:
            error = true;
            log_error("conf: tag token (%d) is disallowed", type);
            break;

        case YAML_BLOCK_SEQUENCE_START_TOKEN:
        case YAML_BLOCK_MAPPING_START_TOKEN:
        case YAML_BLOCK_END_TOKEN:
        case YAML_BLOCK_ENTRY_TOKEN:
            break;

        case YAML_KEY_TOKEN:
        case YAML_VALUE_TOKEN:
        case YAML_SCALAR_TOKEN:
            break;

        case YAML_STREAM_START_TOKEN:
            break;

        case YAML_STREAM_END_TOKEN:
            done = true;
            log_debug(LOG_VVERB, "conf '%s' has valid tokens", cf->fname);
            break;

        default:
            error = true;
            log_error("conf: unknown token (%d) is disallowed", type);
            break;
        }

        conf_token_done(cf);
    } while (!done && !error);

    conf_yaml_deinit(cf);

    return !error ? DN_OK : DN_ERROR;
}

static rstatus_t
conf_validate_structure(struct conf *cf)
{
    rstatus_t status;
    int type, depth;
    uint32_t i, count[CONF_MAX_DEPTH + 1];
    bool done, error, seq;

    status = conf_yaml_init(cf);
    if (status != DN_OK) {
        return status;
    }

    done = false;
    error = false;
    seq = false;
    depth = 0;
    for (i = 0; i < CONF_MAX_DEPTH + 1; i++) {
        count[i] = 0;
    }

    /*
     * Validate that the configuration conforms roughly to the following
     * yaml tree structure:
     *
     * keyx:
     *   key1: value1
     *   key2: value2
     *   seq:
     *     - elem1
     *     - elem2
     *     - elem3
     *   key3: value3
     *
     * keyy:
     *   key1: value1
     *   key2: value2
     *   seq:
     *     - elem1
     *     - elem2
     *     - elem3
     *   key3: value3
     */
    do {
        status = conf_event_next(cf);
        if (status != DN_OK) {
            return status;
        }

        type = cf->event.type;

        log_debug(LOG_VVERB, "next event %d depth %d seq %d", type, depth, seq);

        switch (type) {
        case YAML_STREAM_START_EVENT:
        case YAML_DOCUMENT_START_EVENT:
            break;

        case YAML_DOCUMENT_END_EVENT:
            break;

        case YAML_STREAM_END_EVENT:
            done = true;
            break;

        case YAML_MAPPING_START_EVENT:
            if (depth == CONF_ROOT_DEPTH && count[depth] != 1) {
                error = true;
                log_error("conf: '%s' has more than one \"key:value\" at depth"
                          " %d", cf->fname, depth);
            } else if (depth >= CONF_MAX_DEPTH) {
                error = true;
                log_error("conf: '%s' has a depth greater than %d", cf->fname,
                          CONF_MAX_DEPTH);
            }
            depth++;
            break;

        case YAML_MAPPING_END_EVENT:
            if (depth == CONF_MAX_DEPTH) {
                if (seq) {
                    seq = false;
                //} else {
                //    error = true;
                //    log_error("conf: '%s' missing sequence directive at depth "
                //              "%d", cf->fname, depth);
                }
            }
            depth--;
            count[depth] = 0;
            break;

        case YAML_SEQUENCE_START_EVENT:
            if (seq) {
                error = true;
                log_error("conf: '%s' has more than one sequence directive",
                          cf->fname);
            } else if (depth != CONF_MAX_DEPTH) {
                error = true;
                log_error("conf: '%s' has sequence at depth %d instead of %d",
                          cf->fname, depth, CONF_MAX_DEPTH);
            } else if (count[depth] != 1) {
                error = true;
                log_error("conf: '%s' has invalid \"key:value\" at depth %d",
                          cf->fname, depth);
            }
            seq = true;
            break;

        case YAML_SEQUENCE_END_EVENT:
            ASSERT(depth == CONF_MAX_DEPTH);
            count[depth] = 0;
            seq = false;
            break;

        case YAML_SCALAR_EVENT:
            if (depth == 0) {
                error = true;
                log_error("conf: '%s' has invalid empty \"key:\" at depth %d",
                          cf->fname, depth);
            } else if (depth == CONF_ROOT_DEPTH && count[depth] != 0) {
                error = true;
                log_error("conf: '%s' has invalid mapping \"key:\" at depth %d",
                          cf->fname, depth);
            } else if (depth == CONF_MAX_DEPTH && count[depth] == 2) {
                /* found a "key: value", resetting! */
                count[depth] = 0;
            }
            count[depth]++;
            break;

        default:
            NOT_REACHED();
        }

        conf_event_done(cf);
    } while (!done && !error);

    conf_yaml_deinit(cf);

    return !error ? DN_OK : DN_ERROR;
}

static rstatus_t
conf_pre_validate(struct conf *cf)
{
    rstatus_t status;


    status = conf_validate_document(cf);
    if (status != DN_OK) {
        return status;
    }

    status = conf_validate_tokens(cf);
    if (status != DN_OK) {
        return status;
    }

    status = conf_validate_structure(cf);
    if (status != DN_OK) {
        return status;
    }

    cf->sound = 1;

    return DN_OK;
}

static rstatus_t
conf_validate_server(struct conf *cf, struct conf_pool *cp)
{
    if (cp->conf_datastore == NULL) {
        log_error("conf: pool '%.*s' has no datastores", cp->name.len,
                  cp->name.data);
        return DN_ERROR;
    }

    return DN_OK;
}

/* Validate pool config and set defaults. */
static rstatus_t
conf_validate_pool(struct conf *cf, struct conf_pool *cp)
{
    rstatus_t status;

    ASSERT(!cp->valid);
    ASSERT(!string_empty(&cp->name));

    if (!cp->listen.valid) {
        log_error("conf: directive \"listen:\" is missing");
        return DN_ERROR;
    }

    /* set default values for unset directives */

    if (cp->distribution == CONF_UNSET_DIST) {
        cp->distribution = CONF_DEFAULT_DIST;
    }

    if (string_empty(&cp->dyn_seed_provider)) {
    	string_copy_c(&cp->dyn_seed_provider, (const uint8_t *)CONF_DEFAULT_SEED_PROVIDER);
    }

    if (cp->hash == CONF_UNSET_HASH) {
        cp->hash = CONF_DEFAULT_HASH;
    }

    if (cp->timeout == CONF_UNSET_NUM) {
        cp->timeout = CONF_DEFAULT_TIMEOUT;
    }

    if (cp->backlog == CONF_UNSET_NUM) {
        cp->backlog = CONF_DEFAULT_LISTEN_BACKLOG;
    }

    cp->client_connections = CONF_DEFAULT_CLIENT_CONNECTIONS;

    if (cp->data_store == CONF_UNSET_NUM) {
        cp->data_store = CONF_DEFAULT_DATASTORE;
    }

    if (cp->preconnect == CONF_UNSET_NUM) {
        cp->preconnect = CONF_DEFAULT_PRECONNECT;
    }

    if (cp->auto_eject_hosts == CONF_UNSET_NUM) {
        cp->auto_eject_hosts = CONF_DEFAULT_AUTO_EJECT_HOSTS;
    }

    if (cp->server_connections == CONF_UNSET_NUM) {
        cp->server_connections = CONF_DEFAULT_SERVER_CONNECTIONS;
    } else if (cp->server_connections == 0) {
        log_error("conf: directive \"server_connections:\" cannot be 0");
        return DN_ERROR;
    }

    if (cp->server_retry_timeout_ms == CONF_UNSET_NUM) {
        cp->server_retry_timeout_ms = CONF_DEFAULT_SERVER_RETRY_TIMEOUT;
    }

    if (cp->server_failure_limit == CONF_UNSET_NUM) {
        cp->server_failure_limit = CONF_DEFAULT_SERVER_FAILURE_LIMIT;
    }

    if (cp->dyn_read_timeout == CONF_UNSET_NUM) {
        cp->dyn_read_timeout = CONF_DEFAULT_DYN_READ_TIMEOUT;
    }

    if (cp->dyn_write_timeout == CONF_UNSET_NUM) {
        cp->dyn_write_timeout = CONF_DEFAULT_DYN_WRITE_TIMEOUT;
    }

    if (cp->dyn_connections == CONF_UNSET_NUM) {
        cp->dyn_connections = CONF_DEFAULT_DYN_CONNECTIONS;
    } else if (cp->dyn_connections == 0) {
        log_error("conf: directive \"dyn_connections:\" cannot be 0");
        return DN_ERROR;
    }

    if (cp->gos_interval == CONF_UNSET_NUM) {
        cp->gos_interval = CONF_DEFAULT_GOS_INTERVAL;
    }

    if (cp->conn_msg_rate == CONF_UNSET_NUM) {
        cp->conn_msg_rate = CONF_DEFAULT_CONN_MSG_RATE;
    }

    if (string_empty(&cp->rack)) {
        string_copy_c(&cp->rack, (const uint8_t *)CONF_DEFAULT_RACK);
        log_debug(LOG_INFO, "setting rack to default value:%s", CONF_DEFAULT_RACK);
    }

    if (string_empty(&cp->dc)) {
        string_copy_c(&cp->dc, (const uint8_t *)CONF_DEFAULT_DC);
        log_debug(LOG_INFO, "setting dc to default value:%s", CONF_DEFAULT_DC);
    }

    if (string_empty(&cp->secure_server_option)) {
        string_copy_c(&cp->secure_server_option,
                      (const uint8_t *)CONF_DEFAULT_SECURE_SERVER_OPTION);
        log_debug(LOG_INFO, "setting secure_server_option to default value:%s",
                  CONF_DEFAULT_SECURE_SERVER_OPTION);
    }

    if (string_empty(&cp->read_consistency)) {
        string_copy_c(&cp->read_consistency,
                      (const uint8_t *)CONF_STR_DC_ONE);
        log_debug(LOG_INFO, "setting read_consistency to default value:%s",
                CONF_STR_DC_ONE);
    }

    if (string_empty(&cp->write_consistency)) {
        string_copy_c(&cp->write_consistency,
                      (const uint8_t *)CONF_STR_DC_ONE);
        log_debug(LOG_INFO, "setting write_consistency to default value:%s",
                CONF_STR_DC_ONE);
    }

    if (dn_strcmp(cp->secure_server_option.data, CONF_STR_NONE) &&
        dn_strcmp(cp->secure_server_option.data, CONF_STR_RACK) &&
        dn_strcmp(cp->secure_server_option.data, CONF_STR_DC) &&
        dn_strcmp(cp->secure_server_option.data, CONF_STR_ALL))
    {
        log_error("conf: directive \"secure_server_option:\"must be one of 'none' 'rack' 'datacenter' 'all'");
    }

    if (!dn_strcasecmp(cp->read_consistency.data, CONF_STR_DC_ONE))
        g_read_consistency = DC_ONE;
    else if (!dn_strcasecmp(cp->read_consistency.data, CONF_STR_DC_QUORUM))
        g_read_consistency = DC_QUORUM;
    else {
        log_error("conf: directive \"read_consistency:\"must be one of 'DC_ONE' 'DC_QUORUM'");
        return DN_ERROR;
    }

    if (!dn_strcasecmp(cp->write_consistency.data, CONF_STR_DC_ONE))
        g_write_consistency = DC_ONE;
    else if (!dn_strcasecmp(cp->write_consistency.data, CONF_STR_DC_QUORUM))
        g_write_consistency = DC_QUORUM;
    else {
        log_error("conf: directive \"write_consistency:\"must be one of 'DC_ONE' 'DC_QUORUM'");
        return DN_ERROR;
    }

    if (string_empty(&cp->env)) {
        string_copy_c(&cp->env, (const uint8_t *)CONF_DEFAULT_ENV);
        log_debug(LOG_INFO, "setting env to default value:%s", CONF_DEFAULT_ENV);
    }

    if (string_empty(&cp->pem_key_file)) {
        string_copy_c(&cp->pem_key_file, (const uint8_t *)PEM_KEY_FILE);
        log_debug(LOG_INFO, "setting pem key file to default value:%s", PEM_KEY_FILE);
    }

    status = conf_validate_server(cf, cp);
    if (status != DN_OK) {
        return status;
    }

    cp->valid = 1;

    return DN_OK;
}


/* Determine which peer node communications need to be secured  */
static bool
conf_set_is_secure(struct conf *cf)
{
    bool valid = true;
    uint32_t j, nseeds;

    struct conf_pool *cp = &cf->pool;
    nseeds = array_n(&cp->dyn_seeds);
    for (j = 0; j < nseeds; j++) {
        struct conf_server *cs = array_get(&cp->dyn_seeds, j);
        // if dc then communication only between nodes in different dc is secured
        if (!dn_strcmp(cp->secure_server_option.data, CONF_STR_DC)) {
            if (string_compare(&cp->dc, &cs->dc)) {
                cs->is_secure = 1;
            }
        }
        // if rack then communication only between nodes in different rack is secured.
        // communication secured between nodes if they are in rack with same name across dcs.
        else if (!dn_strcmp(cp->secure_server_option.data, CONF_STR_RACK)) {
            // if not same rack or dc
            if (string_compare(&cp->rack, &cs->rack)
                    || string_compare(&cp->dc, &cs->dc)) {
                cs->is_secure = 1;
            }
        }
        // if all then all communication between nodes will be secured.
        else if (!dn_strcmp(cp->secure_server_option.data, CONF_STR_ALL)) {
            cs->is_secure = 1;
        }
    }

    return valid;
}

static rstatus_t
conf_post_validate(struct conf *cf)
{
    ASSERT(cf->sound && cf->parsed);
    ASSERT(!cf->valid);

    THROW_STATUS(conf_validate_pool(cf, &cf->pool));

    /* Determine which peer node communications need to be secured  */
    if (!conf_set_is_secure(cf)) {
        return DN_ERROR;
    }
    return DN_OK;
}

struct conf *
conf_create(char *filename)
{
    rstatus_t status;
    struct conf *cf;

    cf = conf_open(filename);
    if (cf == NULL) {
        return NULL;
    }

    /* validate configuration file before parsing */
    status = conf_pre_validate(cf);
    if (status != DN_OK) {
        goto error;
    }

    /* parse the configuration file */
    status = conf_parse(cf);
    if (status != DN_OK) {
        goto error;
    }

    /* validate parsed configuration */
    status = conf_post_validate(cf);
    if (status != DN_OK) {
        goto error;
    }

    conf_dump(cf);

    fclose(cf->fh);
    cf->fh = NULL;

    return cf;

error:
	log_stderr("dynomite: configuration file '%s' syntax is invalid", filename);
    fclose(cf->fh);
    cf->fh = NULL;
    conf_destroy(cf);
    return NULL;
}

void
conf_destroy(struct conf *cf)
{
    while (array_n(&cf->arg) != 0) {
        conf_pop_scalar(cf);
    }
    array_deinit(&cf->arg);

    conf_pool_deinit(&cf->pool);
    dn_free(cf);
}

