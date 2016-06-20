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
#include <unistd.h>
#include <sys/types.h>
#include <sys/un.h>
#include <yaml.h>

#include "dyn_core.h"
#include "hashkit/dyn_hashkit.h"


#ifndef _DYN_CONF_H_
#define _DYN_CONF_H_


#define CONF_OK             (void *) NULL
#define CONF_ERROR          (void *) "has an invalid value"

#define CONF_ROOT_DEPTH     1
#define CONF_MAX_DEPTH      CONF_ROOT_DEPTH + 1

#define CONF_DEFAULT_ARGS       3
#define CONF_DEFAULT_POOL       8
#define CONF_DEFAULT_SERVERS    8

#define CONF_UNSET_NUM  -1
#define CONF_UNSET_PTR  NULL
#define CONF_UNSET_HASH (hash_type_t) -1
#define CONF_UNSET_DIST (dist_type_t) -1

#define CONF_DEFAULT_HASH                    HASH_MURMUR
#define CONF_DEFAULT_DIST                    DIST_VNODE
#define CONF_DEFAULT_TIMEOUT                 5000
#define CONF_DEFAULT_LISTEN_BACKLOG          512
#define CONF_DEFAULT_CLIENT_CONNECTIONS      0
#define CONF_DEFAULT_DATASTORE				 0
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
#define CONF_DEFAULT_PEERS                   200

#define CONF_DEFAULT_CONN_MSG_RATE           50000   //conn msgs per sec

#define CONF_STR_NONE                        "none"
#define CONF_STR_DC                          "datacenter"
#define CONF_STR_RACK                        "rack"
#define CONF_STR_ALL                         "all"

#define CONF_STR_DC_ONE                      "dc_one"
#define CONF_STR_DC_QUORUM                   "dc_quorum"
#define CONF_STR_DC_SAFE_QUORUM              "dc_safe_quorum"

#define CONF_DEFAULT_ENV                     "aws"

#define CONF_DEFAULT_RACK                    "localrack"
#define CONF_DEFAULT_DC                      "localdc"
#define CONF_DEFAULT_SECURE_SERVER_OPTION    CONF_STR_NONE

#define CONF_DEFAUTL_SEED_PROVIDER           "simple_provider"

#define PEM_KEY_FILE  "conf/dynomite.pem"


struct conf_listen {
    struct string   pname;   /* listen: as "name:port" */
    struct string   name;    /* name */
    int             port;    /* port */
    struct sockinfo info;    /* listen socket info */
    unsigned        valid:1; /* valid? */
};


struct conf_server {
    struct string   pname;       /* server: as "name:port:weight" */
    struct string   name;        /* name */
    int             port;        /* port */
    int             weight;      /* weight - unused and no config parsing support */
    struct sockinfo info;        /* connect socket info */
    struct array    tokens;      /* tokens for this server */
    struct string   rack;        /* peer node or server's rack */
    struct string   dc;          /* peer node's dc */
    unsigned        valid:1;     /* valid? */
    unsigned        is_secure:1; /* is the connection to the server secure? */
};

struct conf_pool {
    struct string      name;                  /* pool name (root node) */
    struct conf_listen listen;                /* listen: */
    hash_type_t        hash;                  /* hash: */
    struct string      hash_tag;              /* hash_tag: */
    dist_type_t        distribution;          /* distribution: */
    int                timeout;               /* timeout: */
    int                backlog;               /* backlog: */
    int                client_connections;    /* client_connections: */
    int                data_store;            /* data_store: */
    int                preconnect;            /* preconnect: */
    int                auto_eject_hosts;      /* auto_eject_hosts: */
    int                server_connections;    /* server_connections: */
    int                server_retry_timeout;  /* server_retry_timeout: in msec */
    int                server_failure_limit;  /* server_failure_limit: */
    struct array       server;                /* servers: conf_server array */
    unsigned           valid:1;               /* valid? */
    struct conf_listen dyn_listen;            /* dyn_listen  */
    int                dyn_read_timeout;      /* inter dyn nodes' read timeout in ms */
    int                dyn_write_timeout;     /* inter dyn nodes' write timeout in ms */ 
    struct string      dyn_seed_provider;     /* seed provider */ 
    struct array       dyn_seeds;             /* seed nodes: conf_server array */
    int                dyn_port;
    int                dyn_connections;       /* dyn connections */  
    struct string      rack;                  /* this node's logical rack */
    struct array       tokens;                /* this node's token: dyn_token array */
    int                gos_interval;          /* wake up interval in ms */

    /* none | datacenter | rack | all in order of increasing number of connections. (default is datacenter) */
    struct string      secure_server_option;
    struct string      read_consistency;
    struct string      write_consistency;
    struct string      pem_key_file;
    struct string      dc;                    /* this node's dc */
    struct string      env;                   /* aws, google, network, ... */
    int                conn_msg_rate;         /* conn msg per sec */
};


struct conf {
    char          *fname;           /* file name (ref in argv[]) */
    FILE          *fh;              /* file handle */
    struct array  arg;              /* string[] (parsed {key, value} pairs) */
    struct array  pool;             /* conf_pool[] (parsed pools) */
    uint32_t      depth;            /* parsed tree depth */
    yaml_parser_t parser;           /* yaml parser */
    yaml_event_t  event;            /* yaml event */
    yaml_token_t  token;            /* yaml token */
    unsigned      seq:1;            /* sequence? */
    unsigned      valid_parser:1;   /* valid parser? */
    unsigned      valid_event:1;    /* valid event? */
    unsigned      valid_token:1;    /* valid token? */
    unsigned      sound:1;          /* sound? */
    unsigned      parsed:1;         /* parsed? */
    unsigned      valid:1;          /* valid? */
};

struct command {
    struct string name;
    char          *(*set)(struct conf *cf, struct command *cmd, void *data);
    int           offset;
};

#define null_command { null_string, NULL, 0 }

char *conf_set_string(struct conf *cf, struct command *cmd, void *conf);
char *conf_set_listen(struct conf *cf, struct command *cmd, void *conf);
char *conf_add_server(struct conf *cf, struct command *cmd, void *conf);
char *conf_add_dyn_server(struct conf *cf, struct command *cmd, void *conf);
char *conf_set_num(struct conf *cf, struct command *cmd, void *conf);
char *conf_set_bool(struct conf *cf, struct command *cmd, void *conf);
char *conf_set_hash(struct conf *cf, struct command *cmd, void *conf);
char *conf_set_distribution(struct conf *cf, struct command *cmd, void *conf);
char *conf_set_hashtag(struct conf *cf, struct command *cmd, void *conf);
char *conf_set_tokens(struct conf *cf, struct command *cmd, void *conf);

rstatus_t conf_server_each_transform(void *elem, void *data);
rstatus_t conf_pool_each_transform(void *elem, void *data);

rstatus_t conf_seed_each_transform(void *elem, void *data);

struct conf *conf_create(char *filename);
void conf_destroy(struct conf *cf);

#endif
