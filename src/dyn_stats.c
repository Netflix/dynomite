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

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include "dyn_core.h"
#include "dyn_histogram.h"
#include "dyn_server.h"
#include "dyn_ring_queue.h"

struct stats_desc {
    char *name; /* stats name */
    char *desc; /* stats description */
};

#define DEFINE_ACTION(_name, _type, _desc) { .type = _type, .name = string(#_name) },
static struct stats_metric stats_pool_codec[] = {
    STATS_POOL_CODEC( DEFINE_ACTION )
};

static struct stats_metric stats_server_codec[] = {
    STATS_SERVER_CODEC( DEFINE_ACTION )
};
#undef DEFINE_ACTION

#define DEFINE_ACTION(_name, _type, _desc) { .name = #_name, .desc = _desc },
static struct stats_desc stats_pool_desc[] = {
    STATS_POOL_CODEC( DEFINE_ACTION )
};

static struct stats_desc stats_server_desc[] = {
    STATS_SERVER_CODEC( DEFINE_ACTION )
};
#undef DEFINE_ACTION

#define  MAX_HTTP_HEADER_SIZE 1024
static struct string header_str = string("HTTP/1.1 200 OK \nContent-Type: application/json; charset=utf-8 \nContent-Length:");
//static struct string endline = string("\r\n");
static struct string ok = string("OK\r\n");
//static struct string error = string("ERR");

static struct string all = string("all");

static struct histogram latency_histo;

void
stats_describe(void)
{
    uint32_t i;

    log_stderr("pool stats:");
    for (i = 0; i < NELEMS(stats_pool_desc); i++) {
        log_stderr("  %-20s\"%s\"", stats_pool_desc[i].name,
                   stats_pool_desc[i].desc);
    }

    log_stderr("");

    log_stderr("server stats:");
    for (i = 0; i < NELEMS(stats_server_desc); i++) {
        log_stderr("  %-20s\"%s\"", stats_server_desc[i].name,
                   stats_server_desc[i].desc);
    }
}

static void
stats_metric_init(struct stats_metric *stm)
{
    switch (stm->type) {
    case STATS_COUNTER:
        stm->value.counter = 0LL;
        break;

    case STATS_GAUGE:
        stm->value.counter = 0LL;
        break;

    case STATS_TIMESTAMP:
        stm->value.timestamp = 0LL;
        break;

    case STATS_STRING:
        string_init(&stm->value.str);
        break;

    default:
        NOT_REACHED();
    }
}

static void
stats_metric_reset(struct array *stats_metric)
{
    uint32_t i, nmetric;

    nmetric = array_n(stats_metric);
    ASSERT(nmetric == STATS_POOL_NFIELD || nmetric == STATS_SERVER_NFIELD);

    for (i = 0; i < nmetric; i++) {
        struct stats_metric *stm = array_get(stats_metric, i);

        stats_metric_init(stm);
    }
}

static rstatus_t
stats_pool_metric_init(struct array *stats_metric)
{
    rstatus_t status;
    uint32_t i, nfield = STATS_POOL_NFIELD;

    status = array_init(stats_metric, nfield, sizeof(struct stats_metric));
    if (status != DN_OK) {
        return status;
    }

    for (i = 0; i < nfield; i++) {
        struct stats_metric *stm = array_push(stats_metric);

        /* initialize from pool codec first */
        *stm = stats_pool_codec[i];

        /* initialize individual metric */
        stats_metric_init(stm);
    }

    return DN_OK;
}

static rstatus_t
stats_server_metric_init(struct stats_server *sts)
{
    rstatus_t status;
    uint32_t i, nfield = STATS_SERVER_NFIELD;

    status = array_init(&sts->metric, nfield, sizeof(struct stats_metric));
    if (status != DN_OK) {
        return status;
    }

    for (i = 0; i < nfield; i++) {
        struct stats_metric *stm = array_push(&sts->metric);

        /* initialize from server codec first */
        *stm = stats_server_codec[i];

        /* initialize individual metric */
        stats_metric_init(stm);
    }

    return DN_OK;
}

static void
stats_metric_deinit(struct array *metric)
{
    uint32_t i, nmetric;

    nmetric = array_n(metric);
    for (i = 0; i < nmetric; i++) {
        array_pop(metric);
    }
    array_deinit(metric);
}

static rstatus_t
stats_server_init(struct stats_server *sts, struct server *s)
{
    rstatus_t status;

    sts->name = s->name;
    array_null(&sts->metric);

    status = stats_server_metric_init(sts);
    if (status != DN_OK) {
        return status;
    }

    log_debug(LOG_VVVERB, "init stats server '%.*s' with %"PRIu32" metric",
              sts->name.len, sts->name.data, array_n(&sts->metric));

    return DN_OK;

}

static rstatus_t
stats_server_map(struct array *stats_server, struct array *server)
{
    rstatus_t status;
    uint32_t i, nserver;

    nserver = array_n(server);
    ASSERT(nserver != 0);

    status = array_init(stats_server, nserver, sizeof(struct stats_server));
    if (status != DN_OK) {
        return status;
    }

    for (i = 0; i < nserver; i++) {
        struct server *s = array_get(server, i);
        struct stats_server *sts = array_push(stats_server);

        status = stats_server_init(sts, s);
        if (status != DN_OK) {
            return status;
        }
    }

    log_debug(LOG_VVVERB, "map %"PRIu32" stats servers", nserver);

    return DN_OK;
}

static void
stats_server_unmap(struct array *stats_server)
{
    uint32_t i, nserver;

    nserver = array_n(stats_server);

    for (i = 0; i < nserver; i++) {
        struct stats_server *sts = array_pop(stats_server);
        stats_metric_deinit(&sts->metric);
    }
    array_deinit(stats_server);

    log_debug(LOG_VVVERB, "unmap %"PRIu32" stats servers", nserver);
}

static rstatus_t
stats_pool_init(struct stats_pool *stp, struct server_pool *sp)
{
    rstatus_t status;

    stp->name = sp->name;
    array_null(&stp->metric);
    array_null(&stp->server);

    status = stats_pool_metric_init(&stp->metric);
    if (status != DN_OK) {
        return status;
    }

    status = stats_server_map(&stp->server, &sp->server);
    if (status != DN_OK) {
        stats_metric_deinit(&stp->metric);
        return status;
    }

    log_debug(LOG_VVVERB, "init stats pool '%.*s' with %"PRIu32" metric and "
              "%"PRIu32" server", stp->name.len, stp->name.data,
              array_n(&stp->metric), array_n(&stp->metric));

    return DN_OK;
}

static void
stats_pool_reset(struct array *stats_pool)
{
    uint32_t i, npool;

    npool = array_n(stats_pool);

    for (i = 0; i < npool; i++) {
        struct stats_pool *stp = array_get(stats_pool, i);
        uint32_t j, nserver;

        stats_metric_reset(&stp->metric);

        nserver = array_n(&stp->server);
        for (j = 0; j < nserver; j++) {
            struct stats_server *sts = array_get(&stp->server, j);
            stats_metric_reset(&sts->metric);
        }
    }
}

static rstatus_t
stats_pool_map(struct array *stats_pool, struct array *server_pool)
{
    rstatus_t status;
    uint32_t i, npool;

    npool = array_n(server_pool);
    ASSERT(npool != 0);

    status = array_init(stats_pool, npool, sizeof(struct stats_pool));
    if (status != DN_OK) {
        return status;
    }

    for (i = 0; i < npool; i++) {
        struct server_pool *sp = array_get(server_pool, i);
        struct stats_pool *stp = array_push(stats_pool);

        status = stats_pool_init(stp, sp);
        if (status != DN_OK) {
            return status;
        }
    }

    log_debug(LOG_VVVERB, "map %"PRIu32" stats pools", npool);

    return DN_OK;
}

static void
stats_pool_unmap(struct array *stats_pool)
{
    uint32_t i, npool;

    npool = array_n(stats_pool);

    for (i = 0; i < npool; i++) {
        struct stats_pool *stp = array_pop(stats_pool);
        stats_metric_deinit(&stp->metric);
        stats_server_unmap(&stp->server);
    }
    array_deinit(stats_pool);

    log_debug(LOG_VVVERB, "unmap %"PRIu32" stats pool", npool);
}

static rstatus_t
stats_create_buf(struct stats *st)
{
    uint32_t int64_max_digits = 20; /* INT64_MAX = 9223372036854775807 */
    uint32_t int32_max_digits = 10; /* INT32_MAX = 4294967294 */
    uint32_t key_value_extra = 8;   /* "key": "value", */
    uint32_t pool_extra = 8;        /* '"pool_name": { ' + ' }' */
    uint32_t server_extra = 8;      /* '"server_name": { ' + ' }' */
    size_t size = 0;
    uint32_t i;

    ASSERT(st->buf.data == NULL && st->buf.size == 0);

    /* header */
    size += 1;

    size += st->service_str.len;
    size += st->service.len;
    size += key_value_extra;

    size += st->source_str.len;
    size += st->source.len;
    size += key_value_extra;

    size += st->version_str.len;
    size += st->version.len;
    size += key_value_extra;

    size += st->uptime_str.len;
    size += int64_max_digits;
    size += key_value_extra;

    size += st->timestamp_str.len;
    size += int64_max_digits;
    size += key_value_extra;

    size += st->rack_str.len;
    size += st->rack.len;
    size += key_value_extra;

    size += st->dc_str.len;
    size += st->dc.len;
    size += key_value_extra;

    size += st->latency_999th_str.len;
    size += int64_max_digits;
    size += key_value_extra;

    size += st->latency_99th_str.len;
    size += int64_max_digits;
    size += key_value_extra;

    size += st->latency_95th_str.len;
    size += int64_max_digits;
    size += key_value_extra;

    size += st->latency_mean_str.len;
    size += int64_max_digits;
    size += key_value_extra;

    size += st->latency_max_str.len;
    size += int64_max_digits;
    size += key_value_extra;


    size += st->payload_size_999th_str.len;
    size += int32_max_digits;
    size += key_value_extra;

    size += st->payload_size_99th_str.len;
    size += int32_max_digits;
    size += key_value_extra;

    size += st->payload_size_95th_str.len;
    size += int32_max_digits;
    size += key_value_extra;

    size += st->payload_size_mean_str.len;
    size += int32_max_digits;
    size += key_value_extra;

    size += st->payload_size_max_str.len;
    size += int32_max_digits;
    size += key_value_extra;

    size += st->alloc_msgs_str.len;
    size += int32_max_digits;
    size += key_value_extra;

    /* server pools */
    for (i = 0; i < array_n(&st->sum); i++) {
        struct stats_pool *stp = array_get(&st->sum, i);
        uint32_t j;

        size += stp->name.len;
        size += pool_extra;

        for (j = 0; j < array_n(&stp->metric); j++) {
            struct stats_metric *stm = array_get(&stp->metric, j);

            size += stm->name.len;
            size += int64_max_digits;
            size += key_value_extra;
        }

        /* servers per pool */
        for (j = 0; j < array_n(&stp->server); j++) {
            struct stats_server *sts = array_get(&stp->server, j);
            uint32_t k;

            size += sts->name.len;
            size += server_extra;

            for (k = 0; k < array_n(&sts->metric); k++) {
                struct stats_metric *stm = array_get(&sts->metric, k);

                size += stm->name.len;
                size += int64_max_digits;
                size += key_value_extra;
            }
        }

    }

    /* footer */
    size += 2;

    size = DN_ALIGN(size, DN_ALIGNMENT);

    st->buf.data = dn_alloc(size);
    if (st->buf.data == NULL) {
        log_error("create stats buffer of size %zu failed: %s", size,
                   strerror(errno));
        return DN_ENOMEM;
    }
    st->buf.size = size;

    log_debug(LOG_DEBUG, "stats buffer size %zu", size);

    return DN_OK;
}

static void
stats_destroy_buf(struct stats *st)
{
    if (st->buf.size != 0) {
        ASSERT(st->buf.data != NULL);
        dn_free(st->buf.data);
        st->buf.size = 0;
    }
}

static rstatus_t
stats_add_string(struct stats *st, struct string *key, struct string *val)
{
    struct stats_buffer *buf;
    uint8_t *pos;
    size_t room;
    int n;

    buf = &st->buf;
    pos = buf->data + buf->len;
    room = buf->size - buf->len - 1;

    n = dn_snprintf(pos, room, "\"%.*s\":\"%.*s\", ", key->len, key->data,
                    val->len, val->data);
    if (n < 0 || n >= (int)room) {
        return DN_ERROR;
    }

    buf->len += (size_t)n;

    return DN_OK;
}

static rstatus_t
stats_add_num(struct stats *st, struct string *key, int64_t val)
{
    struct stats_buffer *buf;
    uint8_t *pos;
    size_t room;
    int n;

    buf = &st->buf;
    pos = buf->data + buf->len;
    room = buf->size - buf->len - 1;

    n = dn_snprintf(pos, room, "\"%.*s\":%"PRId64", ", key->len, key->data,
                    val);
    if (n < 0 || n >= (int)room) {
        return DN_ERROR;
    }

    buf->len += (size_t)n;

    return DN_OK;
}

static rstatus_t
stats_add_header(struct stats *st)
{
    rstatus_t status;
    struct stats_buffer *buf;
    int64_t cur_ts, uptime;

    buf = &st->buf;
    buf->data[0] = '{';
    buf->len = 1;

    cur_ts = (int64_t)time(NULL);
    uptime = cur_ts - st->start_ts;

    status = stats_add_string(st, &st->service_str, &st->service);
    if (status != DN_OK) {
        return status;
    }

    status = stats_add_string(st, &st->source_str, &st->source);
    if (status != DN_OK) {
        return status;
    }

    status = stats_add_string(st, &st->version_str, &st->version);
    if (status != DN_OK) {
        return status;
    }

    status = stats_add_num(st, &st->uptime_str, uptime);
    if (status != DN_OK) {
        return status;
    }

    status = stats_add_num(st, &st->timestamp_str, cur_ts);
    if (status != DN_OK) {
        return status;
    }

    status = stats_add_string(st, &st->rack_str, &st->rack);
    if (status != DN_OK) {
        return status;
    }

    status = stats_add_string(st, &st->dc_str, &st->dc);
    if (status != DN_OK) {
        return status;
    }

    //latency histogram
    status = stats_add_num(st, &st->latency_max_str, st->latency_histo.val_max);
    if (status != DN_OK) {
        return status;
    }

    status = stats_add_num(st, &st->latency_999th_str, st->latency_histo.val_999th);
    if (status != DN_OK) {
        return status;
    }

    status = stats_add_num(st, &st->latency_99th_str, st->latency_histo.val_99th);
    if (status != DN_OK) {
        return status;
    }

    status = stats_add_num(st, &st->latency_95th_str, st->latency_histo.val_95th);
    if (status != DN_OK) {
        return status;
    }

    status = stats_add_num(st, &st->latency_mean_str, st->latency_histo.mean);
    if (status != DN_OK) {
        return status;
    }

    //payload size histogram
    status = stats_add_num(st, &st->payload_size_max_str, st->payload_size_histo.val_max);
    if (status != DN_OK) {
        return status;
    }

    status = stats_add_num(st, &st->payload_size_999th_str, st->payload_size_histo.val_999th);
    if (status != DN_OK) {
        return status;
    }

    status = stats_add_num(st, &st->payload_size_99th_str, st->payload_size_histo.val_99th);
    if (status != DN_OK) {
        return status;
    }

    status = stats_add_num(st, &st->payload_size_95th_str, st->payload_size_histo.val_95th);
    if (status != DN_OK) {
        return status;
    }

    status = stats_add_num(st, &st->payload_size_mean_str, st->payload_size_histo.mean);
    if (status != DN_OK) {
        return status;
    }

    status = stats_add_num(st, &st->alloc_msgs_str, st->alloc_msgs);
    if (status != DN_OK) {
        return status;
    }

    return DN_OK;
}

static rstatus_t
stats_add_footer(struct stats *st)
{
    struct stats_buffer *buf;
    uint8_t *pos;

    buf = &st->buf;

    if (buf->len == buf->size) {
        return DN_ERROR;
    }

    /* overwrite the last byte and add a new byte */
    pos = buf->data + buf->len - 1;
    pos[0] = '}';
    pos[1] = '\n';
    buf->len += 1;

    return DN_OK;
}

static rstatus_t
stats_begin_nesting(struct stats *st, struct string *key)
{
    struct stats_buffer *buf;
    uint8_t *pos;
    size_t room;
    int n;

    buf = &st->buf;
    pos = buf->data + buf->len;
    room = buf->size - buf->len - 1;

    n = dn_snprintf(pos, room, "\"%.*s\": {", key->len, key->data);
    if (n < 0 || n >= (int)room) {
        return DN_ERROR;
    }

    buf->len += (size_t)n;

    return DN_OK;
}

static rstatus_t
stats_end_nesting(struct stats *st)
{
    struct stats_buffer *buf;
    uint8_t *pos;

    buf = &st->buf;
    pos = buf->data + buf->len;

    pos -= 2; /* go back by 2 bytes */

    switch (pos[0]) {
    case ',':
        /* overwrite last two bytes; len remains unchanged */
        ASSERT(pos[1] == ' ');
        pos[0] = '}';
        pos[1] = ',';
        break;

    case '}':
        if (buf->len == buf->size) {
            return DN_ERROR;
        }
        /* overwrite the last byte and add a new byte */
        ASSERT(pos[1] == ',');
        pos[1] = '}';
        pos[2] = ',';
        buf->len += 1;
        break;

    default:
        NOT_REACHED();
    }

    return DN_OK;
}

static rstatus_t
stats_copy_metric(struct stats *st, struct array *metric)
{
    rstatus_t status;
    uint32_t i;

    for (i = 0; i < array_n(metric); i++) {
        struct stats_metric *stm = array_get(metric, i);

        status = stats_add_num(st, &stm->name, stm->value.counter);
        if (status != DN_OK) {
            return status;
        }
    }

    return DN_OK;
}

static void
stats_aggregate_metric(struct array *dst, struct array *src)
{
    uint32_t i;

    for (i = 0; i < array_n(src); i++) {
        struct stats_metric *stm1, *stm2;

        stm1 = array_get(src, i);
        stm2 = array_get(dst, i);

        ASSERT(stm1->type == stm2->type);

        switch (stm1->type) {
        case STATS_COUNTER:
            stm2->value.counter += stm1->value.counter;
            break;

        case STATS_GAUGE:
            stm2->value.counter += stm1->value.counter;
            break;

        case STATS_TIMESTAMP:
            if (stm1->value.timestamp) {
                stm2->value.timestamp = stm1->value.timestamp;
            }
            break;

        default:
            NOT_REACHED();
        }
    }
}

static void
stats_aggregate(struct stats *st)
{
    uint32_t i;

    if (st->aggregate == 0) {
        log_debug(LOG_PVERB, "skip aggregate of shadow %p to sum %p as "
                  "generator is slow", st->shadow.elem, st->sum.elem);
        return;
    }

    log_debug(LOG_PVERB, "aggregate stats shadow %p to sum %p", st->shadow.elem,
              st->sum.elem);

    for (i = 0; i < array_n(&st->shadow); i++) {
        struct stats_pool *stp1, *stp2;
        uint32_t j;

        stp1 = array_get(&st->shadow, i);
        stp2 = array_get(&st->sum, i);
        stats_aggregate_metric(&stp2->metric, &stp1->metric);

        for (j = 0; j < array_n(&stp1->server); j++) {
            struct stats_server *sts1, *sts2;

            sts1 = array_get(&stp1->server, j);
            sts2 = array_get(&stp2->server, j);
            stats_aggregate_metric(&sts2->metric, &sts1->metric);
        }
    }

    if (st->reset_histogram) {
   	 st->reset_histogram = 0;
   	 histo_reset(&st->latency_histo);
   	 histo_reset(&st->payload_size_histo);
    }
    st->aggregate = 0;
}


static rstatus_t
stats_make_rsp(struct stats *st)
{
    rstatus_t status;
    uint32_t i;

    status = stats_add_header(st);
    if (status != DN_OK) {
        return status;
    }

    for (i = 0; i < array_n(&st->sum); i++) {
        struct stats_pool *stp = array_get(&st->sum, i);
        uint32_t j;

        status = stats_begin_nesting(st, &stp->name);
        if (status != DN_OK) {
            return status;
        }

        /* copy pool metric from sum(c) to buffer */
        status = stats_copy_metric(st, &stp->metric);
        if (status != DN_OK) {
            return status;
        }

        for (j = 0; j < array_n(&stp->server); j++) {
            struct stats_server *sts = array_get(&stp->server, j);

            status = stats_begin_nesting(st, &sts->name);
            if (status != DN_OK) {
                return status;
            }

            /* copy server metric from sum(c) to buffer */
            status = stats_copy_metric(st, &sts->metric);
            if (status != DN_OK) {
                return status;
            }

            status = stats_end_nesting(st);
            if (status != DN_OK) {
                return status;
            }
        }

        status = stats_end_nesting(st);
        if (status != DN_OK) {
            return status;
        }
    }

    status = stats_add_footer(st);
    if (status != DN_OK) {
        return status;
    }

    return DN_OK;
}


static void parse_request(int sd, struct stats_cmd *st_cmd)
{
	size_t max_buf_size = 99999;
	char mesg[max_buf_size], *reqline[3];
	int rcvd;

	memset( (void*)mesg, (int)'\0', max_buf_size );

	rcvd=recv(sd, mesg, max_buf_size, 0);

	if (rcvd < 0) {
		log_debug(LOG_VERB, "stats recv error");
	} else if (rcvd == 0) {   // receive socket closed
		log_debug(LOG_VERB, "Client disconnected upexpectedly");
	} else  {  // message received
		log_debug(LOG_VERB, "%s", mesg);
		reqline[0] = strtok(mesg, " \t\n");
		if ( strncmp(reqline[0], "GET\0", 4) == 0 ) {
			reqline[1] = strtok (NULL, " \t");
			reqline[2] = strtok (NULL, " \t\n");
			log_debug(LOG_VERB, "0: %s\n", reqline[0]);
			log_debug(LOG_VERB, "1: %s\n", reqline[1]);
			log_debug(LOG_VERB, "2: %s\n", reqline[2]);

			if (strncmp( reqline[2], "HTTP/1.0", 8)!=0 && strncmp( reqline[2], "HTTP/1.1", 8)!=0 ) {
				write(sd, "HTTP/1.0 400 Bad Request\n", 25);
				st_cmd->cmd = CMD_UNKNOWN;
				return;
			} else {
				if (strncmp(reqline[1], "/\0", 2) == 0 ) {
					reqline[1] = "/info";
					return;
				} else if (strcmp(reqline[1], "/info") == 0) {
					st_cmd->cmd = CMD_INFO;
					return;
				} else if (strcmp(reqline[1], "/ping") == 0) {
					st_cmd->cmd = CMD_PING;
					return;
				} else if (strcmp(reqline[1], "/describe") == 0) {
					st_cmd->cmd = CMD_DESCRIBE;
					return;
				} else if (strcmp(reqline[1], "/loglevelup") == 0) {
					st_cmd->cmd = CMD_LOG_LEVEL_UP;
					return;
				} else if (strcmp(reqline[1], "/logleveldown") == 0) {
					st_cmd->cmd = CMD_LOG_LEVEL_DOWN;
					return;
				} else if (strcmp(reqline[1], "/historeset") == 0) {
					st_cmd->cmd = CMD_HISTO_RESET;
					return;
				} else if (strncmp(reqline[1], "/peer", 5) == 0) {
					log_debug(LOG_VERB, "Setting peer - URL Parameters : %s", reqline[1]);
					char* peer_state = reqline[1] + 5;
					log_debug(LOG_VERB, "Peer : %s", peer_state);
					if (strncmp(peer_state, "/down", 5) == 0) {
						log_debug(LOG_VERB, "Peer's state is down!");
						st_cmd->cmd = CMD_PEER_DOWN;
						string_init(&st_cmd->req_data);
						string_copy_c(&st_cmd->req_data, peer_state + 6);
					} else if (strncmp(peer_state, "/up", 3) == 0) {
						log_debug(LOG_VERB, "Peer's state is UP!");
						st_cmd->cmd = CMD_PEER_UP;
						string_init(&st_cmd->req_data);
						string_copy_c(&st_cmd->req_data, peer_state + 4);
					} else if (strncmp(peer_state, "/reset", 6) == 0) {
						log_debug(LOG_VERB, "Peer's state is RESET!");
						st_cmd->cmd = CMD_PEER_RESET;
						string_init(&st_cmd->req_data);
						string_copy_c(&st_cmd->req_data, peer_state + 7);
					} else {
						st_cmd->cmd = CMD_PING;
					}

					return;
				}

				if (strncmp(reqline[1], "/state", 6) == 0) {
					log_debug(LOG_VERB, "Setting state - URL Parameters : %s", reqline[1]);
					char* state = reqline[1] + 7;
					log_debug(LOG_VERB, "cmd : %s", state);
					if (strcmp(state, "standby") == 0) {
						st_cmd->cmd = CMD_STANDBY;
						return;
					} else if (strcmp(state, "writes_only") == 0) {
						st_cmd->cmd = CMD_WRITES_ONLY;
						return;
					} else if (strcmp(state, "normal") == 0) {
						st_cmd->cmd = CMD_NORMAL;
						return;
					} else if (strcmp(state, "resuming") == 0) {
						st_cmd->cmd = CMD_RESUMING;
						return;
					}
				}

				st_cmd->cmd = CMD_PING;
				return;
			}
		}
	}

}


static rstatus_t
stats_msg_to_core(stats_cmd_t cmd, void *cb, void *post_cb)
{
    struct stat_msg *msg = dn_alloc(sizeof(*msg));
    msg->cmd = cmd;
    msg->data = NULL;
    msg->cb = cb;
    msg->post_cb = post_cb;
    CBUF_Push(C2S_OutQ, msg);
	return DN_OK;
}

static rstatus_t
stats_http_rsp(int sd, uint8_t *content, size_t len)
{
    ssize_t n;
    uint8_t http_header[MAX_HTTP_HEADER_SIZE];
    memset( (void*)http_header, (int)'\0', MAX_HTTP_HEADER_SIZE );
    n = dn_snprintf(http_header, MAX_HTTP_HEADER_SIZE, "%.*s %u \r\n\r\n", header_str.len, header_str.data, len);

    if (n < 0 || n >= MAX_HTTP_HEADER_SIZE) {
           return DN_ERROR;
    }

    n = dn_sendn(sd, http_header, n);
    if (n < 0) {
       log_error("send http headers on sd %d failed: %s", sd, strerror(errno));
       close(sd);
       return DN_ERROR;
    }

    n = dn_sendn(sd, content, len);

    if (n < 0) {
       log_error("send stats on sd %d failed: %s", sd, strerror(errno));
       close(sd);
       return DN_ERROR;
    }

    close(sd);

    return DN_OK;
}


static rstatus_t
stats_send_rsp(struct stats *st)
{
	rstatus_t status;
	int sd;

	//TODO: move this to an appropriate place that need it.
	status = stats_make_rsp(st);
	if (status != DN_OK) {
		return status;
	}

	sd = accept(st->sd, NULL, NULL);
	if (sd < 0) {
		log_error("accept on m %d failed: %s", st->sd, strerror(errno));
		return DN_ERROR;
	}

	struct stats_cmd st_cmd;

	parse_request(sd, &st_cmd);
	stats_cmd_t cmd = st_cmd.cmd;

	log_debug(LOG_VERB, "cmd %d", cmd);

	if (cmd == CMD_INFO) {
		log_debug(LOG_VERB, "send stats on sd %d %d bytes", sd, st->buf.len);
		return stats_http_rsp(sd, st->buf.data, st->buf.len);
	} else if (cmd == CMD_NORMAL) {
		st->ctx->dyn_state = NORMAL;
		return stats_http_rsp(sd, ok.data, ok.len);
	} else if (cmd == CMD_STANDBY) {
		st->ctx->dyn_state = STANDBY;
		return stats_http_rsp(sd, ok.data, ok.len);
	} else if (cmd == CMD_WRITES_ONLY) {
		st->ctx->dyn_state = WRITES_ONLY;
		return stats_http_rsp(sd, ok.data, ok.len);
	} else if (cmd == CMD_RESUMING) {
		st->ctx->dyn_state = RESUMING;
		return stats_http_rsp(sd, ok.data, ok.len);
	} else if (cmd == CMD_LOG_LEVEL_UP) {
		log_level_up();
		return stats_http_rsp(sd, ok.data, ok.len);
	} else if (cmd == CMD_LOG_LEVEL_DOWN) {
		log_level_down();
		return stats_http_rsp(sd, ok.data, ok.len);
	} else if (cmd == CMD_HISTO_RESET) {
		st->reset_histogram = 1;
		st->updated = 1;
		return stats_http_rsp(sd, ok.data, ok.len);
	} else if (cmd == CMD_PEER_DOWN || cmd == CMD_PEER_UP || cmd == CMD_PEER_RESET) {
		log_debug(LOG_VERB, "st_cmd.req_data '%.*s' ", st_cmd.req_data);
		struct server_pool *sp = array_get(&st->ctx->pool, 0);
		int i, len;

		//I think it is ok to keep this simple without a synchronization
		for (i = 0, len = array_n(&sp->peers); i < len; i++) {
			struct server *peer = array_get(&sp->peers, i);
			log_debug(LOG_VERB, "peer '%.*s' ", peer->name);

			if (string_compare(&st_cmd.req_data, &all) == 0) {
				log_debug(LOG_VERB, "\t\tSetting peer '%.*s' to state %d due to RESET/ALL command", st_cmd.req_data, cmd);
				peer->state = RESET;
			} else if (string_compare(&peer->name, &st_cmd.req_data) == 0) {
				log_debug(LOG_VERB, "\t\tSetting peer '%.*s' to state %d due to RESET command", st_cmd.req_data, cmd);
				switch (cmd) {
				case CMD_PEER_UP:
					peer->state = NORMAL;
					break;
				case CMD_PEER_RESET:
					peer->state = RESET;
					break;
				case CMD_PEER_DOWN:
					peer->state = DOWN;
					break;
				default:
					peer->state = NORMAL;
				}
				break;
			}
		}
		string_deinit(&st_cmd.req_data);
	} else {
		log_debug(LOG_VERB, "Unsupported cmd");
	}

	stats_http_rsp(sd, ok.data, ok.len);
	close(sd);

	return DN_OK;
}

static void
stats_loop_callback(void *arg1, void *arg2)
{
    struct stats *st = arg1;
    int n = *((int *)arg2);

    /* aggregate stats from shadow (b) -> sum (c) */
    stats_aggregate(st);

    if (n == 0) {
        return;
    }

    /* send aggregate stats sum (c) to collector */
    stats_send_rsp(st);
}

static void *
stats_loop(void *arg)
{
    event_loop_stats(stats_loop_callback, arg);
    return NULL;
}

static rstatus_t
stats_listen(struct stats *st)
{
    rstatus_t status;
    struct sockinfo si;

    status = dn_resolve(&st->addr, st->port, &si);
    if (status < 0) {
        return status;
    }

    st->sd = socket(si.family, SOCK_STREAM, 0);
    if (st->sd < 0) {
        log_error("socket failed: %s", strerror(errno));
        return DN_ERROR;
    }

    status = dn_set_reuseaddr(st->sd);
    if (status < 0) {
        log_error("set reuseaddr on m %d failed: %s", st->sd, strerror(errno));
        return DN_ERROR;
    }

    status = bind(st->sd, (struct sockaddr *)&si.addr, si.addrlen);
    if (status < 0) {
        log_error("bind on m %d to addr '%.*s:%u' failed: %s", st->sd,
                  st->addr.len, st->addr.data, st->port, strerror(errno));
        return DN_ERROR;
    }

    status = listen(st->sd, SOMAXCONN);
    if (status < 0) {
        log_error("listen on m %d failed: %s", st->sd, strerror(errno));
        return DN_ERROR;
    }

    log_debug(LOG_NOTICE, "m %d listening on '%.*s:%u'", st->sd,
              st->addr.len, st->addr.data, st->port);

    return DN_OK;
}

static rstatus_t
stats_start_aggregator(struct stats *st)
{
    rstatus_t status;

    if (!stats_enabled) {
        return DN_OK;
    }

    status = stats_listen(st);
    if (status != DN_OK) {
        return status;
    }

    status = pthread_create(&st->tid, NULL, stats_loop, st);
    if (status < 0) {
        log_error("stats aggregator create failed: %s", strerror(status));
        return DN_ERROR;
    }

    return DN_OK;
}

static void
stats_stop_aggregator(struct stats *st)
{
    if (!stats_enabled) {
        return;
    }

    close(st->sd);
}

struct stats *
stats_create(uint16_t stats_port, char *stats_ip, int stats_interval,
             char *source, struct array *server_pool, struct context *ctx)
{
    rstatus_t status;
    struct stats *st;

    st = dn_alloc(sizeof(*st));
    if (st == NULL) {
        return NULL;
    }

    st->port = stats_port;
    st->interval = stats_interval;
    string_set_raw(&st->addr, stats_ip);

    st->start_ts = (int64_t)time(NULL);

    st->buf.len = 0;
    st->buf.data = NULL;
    st->buf.size = 0;

    array_null(&st->current);
    array_null(&st->shadow);
    array_null(&st->sum);

    st->tid = (pthread_t) -1;
    st->sd = -1;

    string_set_text(&st->service_str, "service");
    string_set_text(&st->service, "dynomite");

    string_set_text(&st->source_str, "source");
    string_set_raw(&st->source, source);

    string_set_text(&st->version_str, "version");
    string_set_text(&st->version, DN_VERSION_STRING);

    string_set_text(&st->uptime_str, "uptime");
    string_set_text(&st->timestamp_str, "timestamp");

    //for latency histo
    string_set_text(&st->latency_999th_str, "latency_999th");
    string_set_text(&st->latency_99th_str, "latency_99th");
    string_set_text(&st->latency_95th_str, "latency_95th");
    string_set_text(&st->latency_mean_str, "latency_mean");
    string_set_text(&st->latency_max_str, "latency_max");

    //for payload size histo
    string_set_text(&st->payload_size_999th_str, "payload_size_999th");
    string_set_text(&st->payload_size_99th_str, "payload_size_99th");
    string_set_text(&st->payload_size_95th_str, "payload_size_95th");
    string_set_text(&st->payload_size_mean_str, "payload_size_mean");
    string_set_text(&st->payload_size_max_str, "payload_size_max");

    string_set_text(&st->alloc_msgs_str, "alloc_msgs");

    //only display the first pool
    struct server_pool *sp = (struct server_pool*) array_get(server_pool, 0);

    string_set_text(&st->rack_str, "rack");

    string_copy(&st->rack, sp->rack.data, sp->rack.len);

    string_set_text(&st->dc_str, "dc");
    string_copy(&st->dc, sp->dc.data, sp->dc.len);

    st->updated = 0;
    st->aggregate = 0;

    histo_init(&st->latency_histo);
    histo_init(&st->payload_size_histo);
    st->reset_histogram = 0;
    st->alloc_msgs = 0;

    /* map server pool to current (a), shadow (b) and sum (c) */

    status = stats_pool_map(&st->current, server_pool);
    if (status != DN_OK) {
        goto error;
    }

    status = stats_pool_map(&st->shadow, server_pool);
    if (status != DN_OK) {
        goto error;
    }

    status = stats_pool_map(&st->sum, server_pool);
    if (status != DN_OK) {
        goto error;
    }

    status = stats_create_buf(st);
    if (status != DN_OK) {
        goto error;
    }

    status = stats_start_aggregator(st);
    if (status != DN_OK) {
        goto error;
    }

    st->ctx = ctx;
    return st;

error:
    stats_destroy(st);
    return NULL;
}

void
stats_destroy(struct stats *st)
{
    stats_stop_aggregator(st);
    stats_pool_unmap(&st->sum);
    stats_pool_unmap(&st->shadow);
    stats_pool_unmap(&st->current);
    stats_destroy_buf(st);
    dn_free(st);
}

void
stats_swap(struct stats *st)
{
    if (!stats_enabled) {
        return;
    }

    if (st->aggregate == 1) {
        log_debug(LOG_PVERB, "skip swap of current %p shadow %p as aggregator "
                  "is busy", st->current.elem, st->shadow.elem);
        return;
    }

    if (st->updated == 0) {
        log_debug(LOG_PVERB, "skip swap of current %p shadow %p as there is "
                  "nothing new", st->current.elem, st->shadow.elem);
        return;
    }

    log_debug(LOG_PVERB, "swap stats current %p shadow %p", st->current.elem,
              st->shadow.elem);


    //set the latencies
    histo_compute(&st->latency_histo);

    histo_compute(&st->payload_size_histo);

    st->alloc_msgs = msg_alloc_msgs();

    array_swap(&st->current, &st->shadow);

    /*
     * Reset current (a) stats before giving it back to generator to keep
     * stats addition idempotent
     */
    stats_pool_reset(&st->current);
    st->updated = 0;

    st->aggregate = 1;

}

uint64_t _stats_pool_get_ts(struct context *ctx, struct server_pool *pool,
                     stats_pool_field_t fidx)
{
   struct stats *st = ctx->stats;
   struct stats_pool *stp;
   struct stats_metric *stm;
   uint32_t pidx = pool->idx;

   stp = array_get(&st->current, pidx);
   stm = array_get(&stp->metric, fidx);

   return stm->value.counter;
}

int64_t _stats_pool_get_val(struct context *ctx, struct server_pool *pool,
                     stats_pool_field_t fidx)
{
   struct stats *st = ctx->stats;
   struct stats_pool *stp;
   struct stats_metric *stm;
   uint32_t pidx = pool->idx;

   stp = array_get(&st->current, pidx);
   stm = array_get(&stp->metric, fidx);

   return stm->value.counter;
}


static struct stats_metric *
stats_pool_to_metric(struct context *ctx, struct server_pool *pool,
                     stats_pool_field_t fidx)
{
    struct stats *st;
    struct stats_pool *stp;
    struct stats_metric *stm;
    uint32_t pidx;

    pidx = pool->idx;

    st = ctx->stats;
    stp = array_get(&st->current, pidx);
    stm = array_get(&stp->metric, fidx);

    st->updated = 1;

    log_debug(LOG_VVVERB, "metric '%.*s' in pool %"PRIu32"", stm->name.len,
              stm->name.data, pidx);

    return stm;
}



void
_stats_pool_incr(struct context *ctx, struct server_pool *pool,
                 stats_pool_field_t fidx)
{
    struct stats_metric *stm;

    stm = stats_pool_to_metric(ctx, pool, fidx);

    ASSERT(stm->type == STATS_COUNTER || stm->type == STATS_GAUGE);
    stm->value.counter++;

    log_debug(LOG_VVVERB, "incr field '%.*s' to %"PRId64"", stm->name.len,
              stm->name.data, stm->value.counter);
}




void
_stats_pool_decr(struct context *ctx, struct server_pool *pool,
                 stats_pool_field_t fidx)
{
    struct stats_metric *stm;

    stm = stats_pool_to_metric(ctx, pool, fidx);

    ASSERT(stm->type == STATS_GAUGE);
    stm->value.counter--;

    log_debug(LOG_VVVERB, "decr field '%.*s' to %"PRId64"", stm->name.len,
              stm->name.data, stm->value.counter);
}

void
_stats_pool_incr_by(struct context *ctx, struct server_pool *pool,
                    stats_pool_field_t fidx, int64_t val)
{
    struct stats_metric *stm;

    stm = stats_pool_to_metric(ctx, pool, fidx);

    ASSERT(stm->type == STATS_COUNTER || stm->type == STATS_GAUGE);
    stm->value.counter += val;

    log_debug(LOG_VVVERB, "incr by field '%.*s' to %"PRId64"", stm->name.len,
              stm->name.data, stm->value.counter);
}

void
_stats_pool_decr_by(struct context *ctx, struct server_pool *pool,
                    stats_pool_field_t fidx, int64_t val)
{
    struct stats_metric *stm;

    stm = stats_pool_to_metric(ctx, pool, fidx);

    ASSERT(stm->type == STATS_GAUGE);
    stm->value.counter -= val;

    log_debug(LOG_VVVERB, "decr by field '%.*s' to %"PRId64"", stm->name.len,
              stm->name.data, stm->value.counter);
}

void
_stats_pool_set_ts(struct context *ctx, struct server_pool *pool,
                   stats_pool_field_t fidx, int64_t val)
{
    struct stats_metric *stm;

    stm = stats_pool_to_metric(ctx, pool, fidx);

    ASSERT(stm->type == STATS_TIMESTAMP);
    stm->value.timestamp = val;

    log_debug(LOG_VVVERB, "set ts field '%.*s' to %"PRId64"", stm->name.len,
              stm->name.data, stm->value.timestamp);
}

uint64_t _stats_server_get_ts(struct context *ctx, struct server *server,
      stats_server_field_t fidx)
{
   struct stats *st;
   struct stats_pool *stp;
   struct stats_server *sts;
   struct stats_metric *stm;
   uint32_t pidx, sidx;

   sidx = server->idx;
   pidx = server->owner->idx;

   st = ctx->stats;
   stp = array_get(&st->current, pidx);
   sts = array_get(&stp->server, sidx);
   stm = array_get(&sts->metric, fidx);


   return stm->value.timestamp;
}

void
_stats_pool_set_val(struct context *ctx, struct server_pool *pool,
		              stats_pool_field_t fidx, int64_t val)
{
   struct stats_metric *stm;

   stm = stats_pool_to_metric(ctx, pool, fidx);

   stm->value.counter = val;

   log_debug(LOG_VVVERB, "set val field '%.*s' to %"PRId64"", stm->name.len,
             stm->name.data, stm->value.counter);
}

int64_t _stats_server_get_val(struct context *ctx, struct server *server,
      stats_server_field_t fidx)
{
   struct stats *st;
   struct stats_pool *stp;
   struct stats_server *sts;
   struct stats_metric *stm;
   uint32_t pidx, sidx;

   sidx = server->idx;
   pidx = server->owner->idx;

   st = ctx->stats;
   stp = array_get(&st->current, pidx);
   sts = array_get(&stp->server, sidx);
   stm = array_get(&sts->metric, fidx);


   return stm->value.counter;
}

static struct stats_metric *
stats_server_to_metric(struct context *ctx, struct server *server,
                       stats_server_field_t fidx)
{
    struct stats *st;
    struct stats_pool *stp;
    struct stats_server *sts;
    struct stats_metric *stm;
    uint32_t pidx, sidx;

    sidx = server->idx;
    pidx = server->owner->idx;

    st = ctx->stats;
    stp = array_get(&st->current, pidx);
    sts = array_get(&stp->server, sidx);
    stm = array_get(&sts->metric, fidx);

    st->updated = 1;

    log_debug(LOG_VVVERB, "metric '%.*s' in pool %"PRIu32" server %"PRIu32"",
              stm->name.len, stm->name.data, pidx, sidx);

    return stm;
}

void
_stats_server_incr(struct context *ctx, struct server *server,
                   stats_server_field_t fidx)
{
    
    struct stats_metric *stm;

    stm = stats_server_to_metric(ctx, server, fidx);

    ASSERT(stm->type == STATS_COUNTER || stm->type == STATS_GAUGE);
    stm->value.counter++;

    log_debug(LOG_VVVERB, "incr field '%.*s' to %"PRId64"", stm->name.len,
              stm->name.data, stm->value.counter);
    
}

void
_stats_server_decr(struct context *ctx, struct server *server,
                   stats_server_field_t fidx)
{

    struct stats_metric *stm;

    stm = stats_server_to_metric(ctx, server, fidx);

    ASSERT(stm->type == STATS_GAUGE);
    stm->value.counter--;

    log_debug(LOG_VVVERB, "decr field '%.*s' to %"PRId64"", stm->name.len,
              stm->name.data, stm->value.counter);

}

void
_stats_server_incr_by(struct context *ctx, struct server *server,
                      stats_server_field_t fidx, int64_t val)
{

    struct stats_metric *stm;

    stm = stats_server_to_metric(ctx, server, fidx);

    ASSERT(stm->type == STATS_COUNTER || stm->type == STATS_GAUGE);
    stm->value.counter += val;

    log_debug(LOG_VVVERB, "incr by field '%.*s' to %"PRId64"", stm->name.len,
              stm->name.data, stm->value.counter);

}

void
_stats_server_decr_by(struct context *ctx, struct server *server,
                      stats_server_field_t fidx, int64_t val)
{

    struct stats_metric *stm;

    stm = stats_server_to_metric(ctx, server, fidx);

    ASSERT(stm->type == STATS_GAUGE);
    stm->value.counter -= val;

    log_debug(LOG_VVVERB, "decr by field '%.*s' to %"PRId64"", stm->name.len,
              stm->name.data, stm->value.counter);

}

void
_stats_server_set_ts(struct context *ctx, struct server *server,
                     stats_server_field_t fidx, int64_t val)
{

    struct stats_metric *stm;

    stm = stats_server_to_metric(ctx, server, fidx);

    ASSERT(stm->type == STATS_TIMESTAMP);
    stm->value.timestamp = val;

    log_debug(LOG_VVVERB, "set ts field '%.*s' to %"PRId64"", stm->name.len,
              stm->name.data, stm->value.timestamp);
   
}

//should use macro or something else to make this more elegant
void stats_histo_add_latency(struct context *ctx, uint64_t val)
{
	struct stats *st = ctx->stats;
	histo_add(&st->latency_histo, val);
	ctx->stats->updated = 1;
}

void stats_histo_add_payloadsize(struct context *ctx, uint64_t val)
{
	struct stats *st = ctx->stats;
	histo_add(&st->payload_size_histo, val);
	ctx->stats->updated = 1;
}
