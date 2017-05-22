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

/**
 * @file dyn_conf.h
 * @brief Dynomite configuration (header).
 *
 * Set default configuration values, parse dynomite.yaml, and update the various
 * configuration structs including connections and server pool.
 */
#include <unistd.h>
#include <sys/types.h>
#include <sys/un.h>
#include <yaml.h>

#include "dyn_core.h"
#include "hashkit/dyn_hashkit.h"


#ifndef _DYN_CONF_H_
#define _DYN_CONF_H_

#define CONF_DEFAULT_PEERS                   200
#define CONF_DEFAULT_ENV                     "aws"
#define CONF_DEFAULT_CONN_MSG_RATE           50000   //conn msgs per sec

#define CONF_STR_DC_ONE                      "dc_one"
#define CONF_STR_DC_QUORUM                   "dc_quorum"
#define CONF_STR_DC_SAFE_QUORUM              "dc_safe_quorum"

#define UNSET_NUM                            0

struct conf_listen {
    struct string   pname;   /* listen: as "name:port" */
    struct string   name;    /* name */
    int             port;    /* port */
    struct sockinfo info;    /* listen socket info */
    unsigned        valid:1; /* valid? */
};

/** \struct conf_server
 * Server configuration.
 */
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

/** \struct conf_pool
 * Connection pool configuration.
 */
struct conf_pool {
    struct string      name;                  /* pool name (root node) */
    struct conf_listen listen;                /* listen: */
    hash_type_t        hash;                  /* hash: */
    struct string      hash_tag;              /* hash_tag: */
    void               *deprecated;          /* Deprecated: distribution, server_connections */
    msec_t             timeout;               /* timeout: */
    int                backlog;               /* backlog: */
    int                client_connections;    /* client_connections: */
    int                data_store;            /* data_store: */
    int                preconnect;            /* preconnect: */
    int                auto_eject_hosts;      /* auto_eject_hosts: */
    msec_t             server_retry_timeout_ms;  /* server_retry_timeout: in msec */
    int                server_failure_limit;  /* server_failure_limit: */
    struct conf_server *conf_datastore;       /* This is the underlying datastore */
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
    struct string      recon_key_file;        /* file with Key encryption in reconciliation */
    struct string      recon_iv_file;         /* file with Initialization Vector encryption in reconciliation */
    struct string      dc;                    /* this node's dc */
    struct string      env;                   /* AWS, Google, network, ... */
    uint32_t           conn_msg_rate;         /* conn msg per sec */
    bool               enable_gossip;         /* enable/disable gossip */
    size_t             mbuf_size;             /* mbuf chunk size */
    size_t             alloc_msgs_max;        /* allocated messages buffer size */

    /* stats info */
    int                stats_interval;        /* stats aggregation interval */
    struct conf_listen stats_listen;          /* stats_listen: socket info for stats */

    /* connection pool details */
    uint8_t            datastore_connections;
    uint8_t            local_peer_connections;
    uint8_t            remote_peer_connections;

};


struct conf {
    char          *fname;           /* file name (ref in argv[]) */
    FILE          *fh;              /* file handle */
    struct array  arg;              /* string[] (parsed {key, value} pairs) */
    struct conf_pool pool;          /* conf_pool[] (parsed pools) */
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

#define null_command { null_string, NULL, 0 }

// converts conf_pool to server_pool
rstatus_t conf_pool_transform(struct server_pool *, struct conf_pool *);

struct conf *conf_create(char *filename);
void conf_destroy(struct conf *cf);

#endif
