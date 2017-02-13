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
#include <sys/resource.h>
#include <netinet/in.h>
#include <ctype.h>

#include "dyn_core.h"
#include "dyn_histogram.h"
#include "dyn_server.h"
#include "dyn_node_snitch.h"
#include "dyn_ring_queue.h"
#include "dyn_gossip.h"
#include "dyn_connection.h"
#include "dyn_conf.h"

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
static struct string err_resp = string("ERR");

static struct string all = string("all");

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
        string_deinit(&stm->value.str); // first free the existing data
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
    uint32_t i, nfield = STATS_POOL_NFIELD;

    THROW_STATUS(array_init(stats_metric, nfield, sizeof(struct stats_metric)));

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
    uint32_t i, nfield = STATS_SERVER_NFIELD;

    THROW_STATUS(array_init(&sts->metric, nfield, sizeof(struct stats_metric)));

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
stats_server_init(struct stats_server *sts, struct datastore *s)
{
    sts->name = s->name;
    array_null(&sts->metric);

    THROW_STATUS(stats_server_metric_init(sts));

    log_debug(LOG_VVVERB, "init stats server '%.*s' with %"PRIu32" metric",
              sts->name.len, sts->name.data, array_n(&sts->metric));

    return DN_OK;

}

static rstatus_t
stats_server_map(struct stats_server *sts, struct datastore *datastore)
{
    ASSERT(datastore != NULL);
    THROW_STATUS(stats_server_init(sts, datastore));

    log_debug(LOG_VVVERB, "mapped stats servers");

    return DN_OK;
}

static void
stats_server_unmap(struct stats_server *sts)
{
    stats_metric_deinit(&sts->metric);
    log_debug(LOG_VVVERB, "unmap stats servers");
}

static rstatus_t
stats_pool_init(struct stats_pool *stp, struct server_pool *sp)
{
    rstatus_t status;

    stp->name = sp->name;
    array_null(&stp->metric);

    THROW_STATUS(stats_pool_metric_init(&stp->metric));

    status = stats_server_map(&stp->server, sp->datastore);
    if (status != DN_OK) {
        stats_metric_deinit(&stp->metric);
        return status;
    }

    log_debug(LOG_VVVERB, "init stats pool '%.*s' with %"PRIu32" metric",
              stp->name.len, stp->name.data, array_n(&stp->metric));

    return DN_OK;
}

static void
stats_pool_reset(struct stats_pool *stp)
{
    stats_metric_reset(&stp->metric);

    struct stats_server *sts = &stp->server;
    stats_metric_reset(&sts->metric);
}

static void
stats_pool_unmap(struct stats_pool *stp)
{
    stats_metric_deinit(&stp->metric);
    stats_server_unmap(&stp->server);
}

static rstatus_t
stats_create_bufs(struct stats *st)
{
    uint32_t int64_max_digits = 20; /* INT64_MAX = 9223372036854775807 */
    uint32_t int32_max_digits = 10; /* INT32_MAX = 4294967294 */
    uint32_t key_value_extra = 8;   /* "key": "value", */
    uint32_t pool_extra = 8;        /* '"pool_name": { ' + ' }' */
    uint32_t server_extra = 8;      /* '"server_name": { ' + ' }' */
    size_t size = 0;

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
    size += int64_max_digits;
    size += key_value_extra;

    size += st->free_msgs_str.len;
    size += int64_max_digits;
    size += key_value_extra;

    size += st->alloc_mbufs_str.len;
    size += int64_max_digits;
    size += key_value_extra;

    size += st->free_mbufs_str.len;
    size += int64_max_digits;
    size += key_value_extra;

    size += st->dyn_memory_str.len;
    size += int64_max_digits;
    size += key_value_extra;

    struct stats_pool *stp = &st->sum;
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
    struct stats_server *sts = &stp->server;
    uint32_t k;

    size += sts->name.len;
    size += server_extra;

    for (k = 0; k < array_n(&sts->metric); k++) {
        struct stats_metric *stm = array_get(&sts->metric, k);

        size += stm->name.len;
        size += int64_max_digits;
        size += key_value_extra;
    }

    /* footer */
    size += 2;
    // Accomodate for new fields that are directly added using stats_add_num_str
    size += 1024;

    size = DN_ALIGN(size, DN_ALIGNMENT);
    st->buf.data = dn_alloc(size);
    if (st->buf.data == NULL) {
        log_error("create stats buffer of size %zu failed: %s", size,
                   strerror(errno));
        return DN_ENOMEM;
    }
    st->buf.size = size;
    st->buf.len = 0;

    log_debug(LOG_DEBUG, "stats info buffer size %zu", size);

    st->clus_desc_buf.len = 0;
    st->clus_desc_buf.size = 0;

    return DN_OK;
}

static void
stats_destroy_buf(struct stats_buffer *buf)
{
    if (buf->size != 0) {
        ASSERT(buf->data != NULL);
        dn_free(buf->data);
        buf->size = 0;
    }
}

static void
stats_reset_buf(struct stats_buffer *buf)
{
    buf->len = 0;
}

static rstatus_t
stats_add_string(struct stats_buffer *buf, struct string *key, struct string *val)
{
    uint8_t *pos;
    size_t room;
    int n;

    pos = buf->data + buf->len;
    room = buf->size - buf->len - 1;

    n = dn_snprintf(pos, room, "\"%.*s\":\"%.*s\",", key->len, key->data,
                    val->len, val->data);
    if (n < 0 || n >= (int)room) {
        log_debug(LOG_ERR, "no room size:%u len %u", buf->size, buf->len);
        return DN_ERROR;
    }

    buf->len += (size_t)n;

    return DN_OK;
}

static rstatus_t
stats_add_num_last(struct stats_buffer *buf, struct string *key, int64_t val, bool last)
{
    uint8_t *pos;
    size_t room;
    int n;

    pos = buf->data + buf->len;
    room = buf->size - buf->len - 1;

    if (!last) {
        n = dn_snprintf(pos, room, "\"%.*s\":%"PRId64",\n", key->len, key->data,
                       val);
    } else {
        n = dn_snprintf(pos, room, "\"%.*s\":%"PRId64"\n", key->len, key->data,
                       val);
    }

    if (n < 0 || n >= (int)room) {
        log_debug(LOG_ERR, "no room size:%u len %u", buf->size, buf->len);
        return DN_ERROR;
    }

    buf->len += (size_t)n;

    return DN_OK;
}

static rstatus_t
stats_add_num_str(struct stats_buffer *buf, const char *key, int64_t val)
{
    uint8_t *pos;
    size_t room;
    int n;

    pos = buf->data + buf->len;
    room = buf->size - buf->len - 1;

    n = dn_snprintf(pos, room, "\"%s\":%"PRId64",\n", key, val);
    if (n < 0 || n >= (int)room) {
        log_debug(LOG_ERR, "no room size:%u len %u", buf->size, buf->len);
        return DN_ERROR;
    }
    buf->len += (size_t)n;
    return DN_OK;
}

static rstatus_t
stats_add_num(struct stats_buffer *buf, struct string *key, int64_t val)
{
	if (stats_add_num_last(buf,key, val, false) == DN_ERROR) {
		return DN_ERROR;
	}

	return DN_OK;

}

static rstatus_t
stats_add_header(struct stats *st)
{
    struct stats_buffer *buf;
    int64_t cur_ts, uptime;

    buf = &st->buf;
    buf->data[0] = '{';
    buf->len = 1;

    cur_ts = (int64_t)time(NULL);
    uptime = cur_ts - st->start_ts;

    THROW_STATUS(stats_add_string(&st->buf, &st->service_str, &st->service));
    THROW_STATUS(stats_add_string(&st->buf, &st->source_str, &st->source));
    THROW_STATUS(stats_add_string(&st->buf, &st->version_str, &st->version));
    THROW_STATUS(stats_add_num(&st->buf, &st->uptime_str, uptime));
    THROW_STATUS(stats_add_num(&st->buf, &st->timestamp_str, cur_ts));
    THROW_STATUS(stats_add_string(&st->buf, &st->rack_str, &st->rack));
    THROW_STATUS(stats_add_string(&st->buf, &st->dc_str, &st->dc));
    //latency histogram
    THROW_STATUS(stats_add_num(&st->buf, &st->latency_max_str,
                 (int64_t)st->latency_histo.val_max))
    THROW_STATUS(stats_add_num(&st->buf, &st->latency_999th_str,
                 (int64_t)st->latency_histo.val_999th));
    THROW_STATUS(stats_add_num(&st->buf, &st->latency_99th_str,
                 (int64_t)st->latency_histo.val_99th));
    THROW_STATUS(stats_add_num(&st->buf, &st->latency_95th_str,
                 (int64_t)st->latency_histo.val_95th));
    THROW_STATUS(stats_add_num(&st->buf, &st->latency_mean_str,
                 (int64_t)st->latency_histo.mean));
    //payload size histogram
    THROW_STATUS(stats_add_num(&st->buf, &st->payload_size_max_str,
                 (int64_t)st->payload_size_histo.val_max));
    THROW_STATUS(stats_add_num(&st->buf, &st->payload_size_999th_str,
                 (int64_t)st->payload_size_histo.val_999th));
    THROW_STATUS(stats_add_num(&st->buf, &st->payload_size_99th_str,
                 (int64_t)st->payload_size_histo.val_99th));
    THROW_STATUS(stats_add_num(&st->buf, &st->payload_size_95th_str,
                 (int64_t)st->payload_size_histo.val_95th));
    THROW_STATUS(stats_add_num(&st->buf, &st->payload_size_mean_str,
                 (int64_t)st->payload_size_histo.mean));

    THROW_STATUS(stats_add_num_str(&st->buf, "average_cross_region_rtt",
                (int64_t)st->cross_region_latency_histo.mean));
    THROW_STATUS(stats_add_num_str(&st->buf, "99_cross_region_rtt",
                (int64_t)st->cross_region_latency_histo.val_99th));
    THROW_STATUS(stats_add_num_str(&st->buf, "average_cross_zone_latency",
                (int64_t)st->cross_zone_latency_histo.mean));
    THROW_STATUS(stats_add_num_str(&st->buf, "99_cross_zone_latency",
                (int64_t)st->cross_zone_latency_histo.val_99th));
    THROW_STATUS(stats_add_num_str(&st->buf, "average_server_latency",
                (int64_t)st->server_latency_histo.mean));
    THROW_STATUS(stats_add_num_str(&st->buf, "99_server_latency",
                (int64_t)st->server_latency_histo.val_99th));

    THROW_STATUS(stats_add_num_str(&st->buf, "average_cross_region_queue_wait",
                (int64_t)st->cross_region_queue_wait_time_histo.mean));
    THROW_STATUS(stats_add_num_str(&st->buf, "99_cross_region_queue_wait",
                (int64_t)st->cross_region_queue_wait_time_histo.val_99th));
    THROW_STATUS(stats_add_num_str(&st->buf, "average_cross_zone_queue_wait",
                (int64_t)st->cross_zone_queue_wait_time_histo.mean));
    THROW_STATUS(stats_add_num_str(&st->buf, "99_cross_zone_queue_wait",
                (int64_t)st->cross_zone_queue_wait_time_histo.val_99th));
    THROW_STATUS(stats_add_num_str(&st->buf, "average_server_queue_wait",
                (int64_t)st->server_queue_wait_time_histo.mean));
    THROW_STATUS(stats_add_num_str(&st->buf, "99_server_queue_wait",
                (int64_t)st->server_queue_wait_time_histo.val_99th));

    THROW_STATUS(stats_add_num(&st->buf, &st->client_out_queue_99,
                 (int64_t)st->client_out_queue.val_99th));
    THROW_STATUS(stats_add_num(&st->buf, &st->server_in_queue_99,
                 (int64_t)st->server_in_queue.val_99th));
    THROW_STATUS(stats_add_num(&st->buf, &st->server_out_queue_99,
                 (int64_t)st->server_out_queue.val_99th));
    THROW_STATUS(stats_add_num(&st->buf, &st->dnode_client_out_queue_99,
                 (int64_t)st->dnode_client_out_queue.val_99th));
    THROW_STATUS(stats_add_num(&st->buf, &st->peer_in_queue_99,
                 (int64_t)st->peer_in_queue.val_99th));
    THROW_STATUS(stats_add_num(&st->buf, &st->peer_out_queue_99,
                 (int64_t)st->peer_out_queue.val_99th));
    THROW_STATUS(stats_add_num(&st->buf, &st->remote_peer_out_queue_99,
                 (int64_t)st->remote_peer_out_queue.val_99th));
    THROW_STATUS(stats_add_num(&st->buf, &st->remote_peer_in_queue_99,
                 (int64_t)st->remote_peer_in_queue.val_99th));
    THROW_STATUS(stats_add_num(&st->buf, &st->alloc_msgs_str,
                 (int64_t)st->alloc_msgs));
    THROW_STATUS(stats_add_num(&st->buf, &st->free_msgs_str,
                 (int64_t)st->free_msgs));
    THROW_STATUS(stats_add_num(&st->buf, &st->alloc_mbufs_str,
                 (int64_t)st->alloc_mbufs));
    THROW_STATUS(stats_add_num(&st->buf, &st->free_mbufs_str,
                 (int64_t)st->free_mbufs));
    THROW_STATUS(stats_add_num(&st->buf, &st->dyn_memory_str,
                 (int64_t)st->dyn_memory));

    return DN_OK;
}

static rstatus_t
stats_add_footer(struct stats_buffer *buf)
{
    uint8_t *pos;

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
stats_begin_nesting(struct stats_buffer *buf, struct string *key, bool arr)
{
    uint8_t *pos;
    size_t room;
    int n;

    pos = buf->data + buf->len;
    room = buf->size - buf->len - 1;

    if (key)
        n = dn_snprintf(pos, room, "\"%.*s\": %c", key->len, key->data,
                        arr ? '[' : '{');
    else
        n = dn_snprintf(pos, room, "%c", arr ? '[' : '{');
    if (n < 0 || n >= (int)room) {
        log_debug(LOG_ERR, "failed, len:%u size %u", buf->len, buf->size);
        return DN_ERROR;
    }

    buf->len += (size_t)n;

    return DN_OK;
}

static rstatus_t
stats_end_nesting(struct stats_buffer *buf, bool arr)
{
    uint8_t *pos;

    pos = buf->data + buf->len;

    // if last non-white space character is , remove it
    // first count white spaces at end
    int space_count = 0;
    while (isspace(*(pos - space_count - 1))) {
        space_count++;
    }
    if (*(pos - space_count - 1) == ',') {
        // now remove , from end
        pos -= (space_count + 1);
        buf->len--;
        // put white spaces back
        while (space_count > 0) {
            *pos = *(pos + 1);
            pos++;
            space_count--;
        }
    }
    // append "},"
    if ((buf->len + 2) > buf->size) {
        return DN_ERROR;
    }
    pos[0] = arr ? ']' : '}';
    pos[1] = ',';
    buf->len += 2;

    return DN_OK;
}

static rstatus_t
stats_copy_metric(struct stats *st, struct array *metric, bool trim_comma)
{
    uint32_t i;

    // Do not include last element in loop as we need to check if it gets a comma
    for (i = 0; i < array_n(metric) - 1; i++) {
        struct stats_metric *stm = array_get(metric, i);
        THROW_STATUS(stats_add_num(&st->buf, &stm->name, stm->value.counter));
    }

    // Last metric inside dyn_o_mite:{} does not get a comma
    struct stats_metric *stm = array_get(metric, array_n(metric) - 1);
    THROW_STATUS(stats_add_num_last(&st->buf, &stm->name, stm->value.counter, trim_comma));

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
    if (st->aggregate == 0) {
        log_debug(LOG_PVERB, "skip aggregate of shadow to sum as generator is slow");
        return;
    }

    log_debug(LOG_PVERB, "aggregate stats shadow %p to sum %p", &st->shadow,
              &st->sum);

    struct stats_pool  *stp1 = &st->shadow;
    struct stats_pool  *stp2 = &st->sum;
    stats_aggregate_metric(&st->sum.metric, &st->shadow.metric);

    struct stats_server *sts1, *sts2;

    sts1 = &stp1->server;
    sts2 = &stp2->server;
    stats_aggregate_metric(&sts2->metric, &sts1->metric);

    static msec_t last_reset = 0;
    if (!last_reset)
        last_reset = dn_msec_now();
    if ((last_reset + 5*60*1000) < dn_msec_now()) {
        st->reset_histogram = 1;
        last_reset = dn_msec_now();
    }
    if (st->reset_histogram) {
        st->reset_histogram = 0;
        histo_reset(&st->latency_histo);
        histo_reset(&st->payload_size_histo);

        histo_reset(&st->server_latency_histo);
        histo_reset(&st->cross_zone_latency_histo);
        histo_reset(&st->cross_region_latency_histo);

        histo_reset(&st->server_queue_wait_time_histo);
        histo_reset(&st->cross_zone_queue_wait_time_histo);
        histo_reset(&st->cross_region_queue_wait_time_histo);

        histo_reset(&st->server_in_queue);
        histo_reset(&st->server_out_queue);
        histo_reset(&st->client_out_queue);
        histo_reset(&st->dnode_client_out_queue);
        histo_reset(&st->peer_in_queue);
        histo_reset(&st->peer_out_queue);
        histo_reset(&st->remote_peer_in_queue);
        histo_reset(&st->remote_peer_out_queue);
    }
    st->aggregate = 0;
}

static rstatus_t
stats_make_info_rsp(struct stats *st)
{

    THROW_STATUS(stats_add_header(st));

    struct stats_pool *stp = &st->sum;

    THROW_STATUS(stats_begin_nesting(&st->buf, &stp->name, false));
    /* copy pool metric from sum(c) to buffer */
    THROW_STATUS(stats_copy_metric(st, &stp->metric, false));

    struct string stats_servers_list;
    string_set_text(&stats_servers_list, "servers")

    struct stats_server *sts = &stp->server;

    THROW_STATUS(stats_begin_nesting(&st->buf, &stats_servers_list, false));
    THROW_STATUS(stats_begin_nesting(&st->buf, &sts->name, false));
    /* copy server metric from sum(c) to buffer */
    THROW_STATUS(stats_copy_metric(st, &sts->metric, true));
    THROW_STATUS(stats_end_nesting(&st->buf, false));
    THROW_STATUS(stats_end_nesting(&st->buf, false));

    THROW_STATUS(stats_end_nesting(&st->buf, false));
    THROW_STATUS(stats_add_footer(&st->buf));

    return DN_OK;
}

static rstatus_t
get_host_from_pname(struct string *host, struct string *pname)
{
    uint8_t *found = dn_strchr(pname->data,
                               &pname->data[pname->len], ':');
    string_init(host);
    if (found) {
        size_t hostlen = found - pname->data;
        THROW_STATUS(string_copy(host, pname->data, hostlen));
        return DN_OK;
    }
    THROW_STATUS(string_copy(host, pname->data, pname->len));
    return DN_OK;
}

static rstatus_t
stats_add_node_host(struct stats *st, struct gossip_node *node)
{
    struct string host_str;
    string_set_text(&host_str, "host");
    struct server_pool *sp = &st->ctx->pool;
    struct string host;
    // pname is host:port. for local its 0.0.0.0:port
    // so try to get the hostname if local otherwise use whats in pname
    char *hn = NULL;
    if (node->is_local && (hn = get_public_hostname(sp))) {
        THROW_STATUS(string_copy(&host, hn, dn_strlen(hn)));
    } else
        get_host_from_pname(&host, &node->pname);

    THROW_STATUS(stats_add_string(&st->clus_desc_buf, &host_str,
                                 &host));
    string_deinit(&host);
    return DN_OK;
}

static rstatus_t
stats_add_node_name(struct stats *st, struct gossip_node *node)
{
    struct string name_str;
    string_set_text(&name_str, "name");
    struct server_pool *sp = &st->ctx->pool;
    // name is the ip address
    if (node->is_local) {
        // get the ip aka name
        struct string ip;
        char * ip4 = get_public_ip4(sp);
        if (ip4) {
            string_set_raw(&ip, ip4);
            THROW_STATUS(stats_add_string(&st->clus_desc_buf, &name_str, &ip));
        } else
            THROW_STATUS(stats_add_string(&st->clus_desc_buf, &name_str,
                                          &node->name));
    } else {
        THROW_STATUS(stats_add_string(&st->clus_desc_buf, &name_str,
                                      &node->name));
    }
    return DN_OK;
}

static rstatus_t
stats_add_node_details(struct stats *st, struct gossip_node *node)
{
    struct string port_str, token_str;
    string_set_text(&port_str, "port");
    string_set_text(&token_str, "token");
    
    THROW_STATUS(stats_add_node_name(st, node));
    THROW_STATUS(stats_add_node_host(st, node));
    THROW_STATUS(stats_add_num(&st->clus_desc_buf, &port_str, node->port));
    THROW_STATUS(stats_add_num_last(&st->clus_desc_buf, &token_str, *(node->token.mag), true));
    return DN_OK;
}

static rstatus_t
stats_add_rack_details(struct stats *st, struct gossip_rack *rack)
{
    struct string name_str, servers_str;
    string_set_text(&name_str, "name");
    string_set_text(&servers_str, "servers");
    THROW_STATUS(stats_add_string(&st->clus_desc_buf, &name_str, &rack->name));
    // servers : [
    THROW_STATUS(stats_begin_nesting(&st->clus_desc_buf, &servers_str, true));
    uint32_t ni;
    for(ni = 0; ni < array_n(&rack->nodes); ni++) {
        struct gossip_node *node = array_get(&rack->nodes, ni);
        THROW_STATUS(stats_begin_nesting(&st->clus_desc_buf, NULL, false));
        THROW_STATUS(stats_add_node_details(st, node));
        THROW_STATUS(stats_end_nesting(&st->clus_desc_buf, false));
    }
    THROW_STATUS(stats_end_nesting(&st->clus_desc_buf, true));
    return DN_OK;
}

static rstatus_t
stats_add_dc_details(struct stats *st, struct gossip_dc *dc)
{
    struct string name_str, racks_str;
    string_set_text(&name_str, "name");
    string_set_text(&racks_str, "racks");

    THROW_STATUS(stats_add_string(&st->clus_desc_buf, &name_str, &dc->name));
    // racks : [
    THROW_STATUS(stats_begin_nesting(&st->clus_desc_buf, &racks_str, true));
    uint32_t ri;
    for(ri = 0; ri < array_n(&dc->racks); ri++) {
        struct gossip_rack *rack = array_get(&dc->racks, ri);

        THROW_STATUS(stats_begin_nesting(&st->clus_desc_buf, NULL, false));
        THROW_STATUS(stats_add_rack_details(st, rack));
        THROW_STATUS(stats_end_nesting(&st->clus_desc_buf, false));
    }
    THROW_STATUS(stats_end_nesting(&st->clus_desc_buf, true));
    return DN_OK;
}

static rstatus_t
stats_resize_clus_desc_buf(struct stats *st)
{
    struct server_pool *sp = &st->ctx->pool;
    ASSERT(sp);
    size_t size = 1024 * array_n(&sp->peers);
    size = DN_ALIGN(size, DN_ALIGNMENT);
    if (st->clus_desc_buf.size < size) {
        stats_destroy_buf(&st->clus_desc_buf);
        st->clus_desc_buf.data = dn_alloc(size);
        if (st->clus_desc_buf.data == NULL) {
            log_error("create cluster desc buffer of size %zu failed: %s",
                      size, strerror(errno));
            return DN_ENOMEM;
        }
        st->clus_desc_buf.size = size;
    }

    stats_reset_buf(&st->clus_desc_buf);
    return DN_OK;
}

static rstatus_t
stats_make_cl_desc_rsp(struct stats *st)
{
    THROW_STATUS(stats_resize_clus_desc_buf(st));
    THROW_STATUS(stats_begin_nesting(&st->clus_desc_buf, NULL, false));

    struct string dcs_str;
    string_set_text(&dcs_str, "dcs");
    THROW_STATUS(stats_begin_nesting(&st->clus_desc_buf, &dcs_str, true));
    uint32_t di;
    for(di = 0; di < array_n(&gn_pool.datacenters); di++) {
        struct gossip_dc *dc = array_get(&gn_pool.datacenters, di);

        THROW_STATUS(stats_begin_nesting(&st->clus_desc_buf, NULL, false));
        THROW_STATUS(stats_add_dc_details(st, dc));
        THROW_STATUS(stats_end_nesting(&st->clus_desc_buf, false));

    }

    THROW_STATUS(stats_end_nesting(&st->clus_desc_buf, true));
    THROW_STATUS(stats_add_footer(&st->clus_desc_buf));
    return DN_OK;
}


static void
parse_request(int sd, struct stats_cmd *st_cmd)
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

            if (!reqline[1] || !reqline[2] ||
                (strncmp( reqline[2], "HTTP/1.0", 8)!=0 &&
                 strncmp( reqline[2], "HTTP/1.1", 8)!=0)) {
                ssize_t wrote = write(sd, "HTTP/1.0 400 Bad Request\n", 25);
                IGNORE_RET_VAL(wrote);
                st_cmd->cmd = CMD_UNKNOWN;
                return;
            } else {
                if (strncmp(reqline[1], "/\0", 2) == 0 ) {
                    reqline[1] = "/info";
                    return;
                } else if (strcmp(reqline[1], "/info") == 0) {
                    st_cmd->cmd = CMD_INFO;
                    return;
                } else if (strcmp(reqline[1], "/help") == 0) {
                    st_cmd->cmd = CMD_HELP;
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
                } else if (strcmp(reqline[1], "/cluster_describe") == 0) {
                    st_cmd->cmd = CMD_CL_DESCRIBE;
                    return;
                } else if (strcmp(reqline[1], "/get_consistency") == 0) {
                    st_cmd->cmd = CMD_GET_CONSISTENCY;
                    return;
                } else if (strncmp(reqline[1], "/set_consistency", 16) == 0) {
                    st_cmd->cmd = CMD_SET_CONSISTENCY;
                    log_notice("Setting consistency parameters: %s", reqline[1]);
                    char* op = reqline[1] + 16;
                    if (strncmp(op, "/read", 5) == 0) {
                        char* type = op + 5;
                        log_notice("op: %s", op);
                        log_notice("type: %s", type);
                        if (!dn_strcasecmp(type, "/"CONF_STR_DC_ONE))
                            g_read_consistency = DC_ONE;
                        else if (!dn_strcasecmp(type, "/"CONF_STR_DC_QUORUM))
                            g_read_consistency = DC_QUORUM;
                        else if (!dn_strcasecmp(type, "/"CONF_STR_DC_SAFE_QUORUM))
                            g_read_consistency = DC_SAFE_QUORUM;
                        else
                            st_cmd->cmd = CMD_UNKNOWN;
                    } else if (strncmp(op, "/write", 6) == 0) {
                        char* type = op + 6;
                        if (!dn_strcasecmp(type, "/"CONF_STR_DC_ONE))
                            g_write_consistency = DC_ONE;
                        else if (!dn_strcasecmp(type, "/"CONF_STR_DC_QUORUM))
                            g_write_consistency = DC_QUORUM;
                        else if (!dn_strcasecmp(type, "/"CONF_STR_DC_SAFE_QUORUM))
                            g_write_consistency = DC_SAFE_QUORUM;
                        else
                            st_cmd->cmd = CMD_UNKNOWN;
                    } else
                        st_cmd->cmd = CMD_UNKNOWN;
                    return;
                } else if (strcmp(reqline[1], "/get_timeout_factor") == 0) {
                    st_cmd->cmd = CMD_GET_TIMEOUT_FACTOR;
                    return;
                } else if (strncmp(reqline[1], "/set_timeout_factor", 19) == 0) {
                    st_cmd->cmd = CMD_SET_TIMEOUT_FACTOR;
                    log_notice("Setting timeout factor: %s", reqline[1]);
                    char* val = reqline[1] + 19;
                    if (*val != '/') {
                        st_cmd->cmd = CMD_UNKNOWN;
                        return;
                    } else {
                        val++;
                        string_init(&st_cmd->req_data);
                        string_copy_c(&st_cmd->req_data, val);
                    }
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
                    log_debug(LOG_VERB, "Setting/Getting state - URL Parameters : %s", reqline[1]);
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
                    } else if (strcmp(state, "get_state") == 0) {
                        st_cmd->cmd = CMD_GET_STATE;
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
stats_http_rsp(int sd, uint8_t *content, size_t len)
{
    ssize_t n;
    uint8_t http_header[MAX_HTTP_HEADER_SIZE];
    memset( (void*)http_header, (int)'\0', MAX_HTTP_HEADER_SIZE );
    n = dn_snprintf(http_header, MAX_HTTP_HEADER_SIZE, "%.*s %lu \r\n\r\n", header_str.len, header_str.data, len);

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
    int sd;

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
        if (stats_make_info_rsp(st) != DN_OK)
            return stats_http_rsp(sd, err_resp.data, err_resp.len);
        else  {
            log_debug(LOG_VERB, "send stats on sd %d %d bytes", sd, st->buf.len);
            return stats_http_rsp(sd, st->buf.data, st->buf.len);
        }
    } else if (cmd == CMD_HELP) {
        char rsp[5120];
        dn_sprintf(rsp, "/info\n/help\n/ping\n/cluster_describe\n/standby\n"\
                        "/writes_only\n/loglevelup\n/logleveldown\n/historeset\n"\
                        "/get_consistency\n/set_consistency/<read|write>/<dc_one|dc_quorum>\n"\
                        "/get_timeout_factor\n/set_timeout_factor/<1-10>\n/peer/<up|down|reset>\n"\
                        "/state/<get_state|writes_only|normal|%s>\n\n", "resuming");
        return stats_http_rsp(sd, rsp, dn_strlen(rsp));
    } else if (cmd == CMD_NORMAL) {
        core_set_local_state(st->ctx, NORMAL);
        return stats_http_rsp(sd, ok.data, ok.len);
    } else if (cmd == CMD_CL_DESCRIBE) {
        if (stats_make_cl_desc_rsp(st) != DN_OK)
            return stats_http_rsp(sd, err_resp.data, err_resp.len);
        else
            return stats_http_rsp(sd, st->clus_desc_buf.data, st->clus_desc_buf.len);
    } else if (cmd == CMD_STANDBY) {
        core_set_local_state(st->ctx, STANDBY);
        return stats_http_rsp(sd, ok.data, ok.len);
    } else if (cmd == CMD_WRITES_ONLY) {
        core_set_local_state(st->ctx, WRITES_ONLY);
        return stats_http_rsp(sd, ok.data, ok.len);
    } else if (cmd == CMD_RESUMING) {
        core_set_local_state(st->ctx, RESUMING);
        return stats_http_rsp(sd, ok.data, ok.len);
    } else if (cmd == CMD_GET_STATE) {
        char rsp[1024];
        dn_sprintf(rsp, "State: %s\n", get_state(st->ctx->dyn_state));
        return stats_http_rsp(sd, rsp, dn_strlen(rsp));
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
    } else if (cmd == CMD_GET_CONSISTENCY) {
        char cons_rsp[1024];
        dn_sprintf(cons_rsp, "Read Consistency: %s\r\nWrite Consistency: %s\r\n",
                   get_consistency_string(g_read_consistency),
                   get_consistency_string(g_write_consistency));
        return stats_http_rsp(sd, cons_rsp, dn_strlen(cons_rsp));
    } else if (cmd == CMD_GET_TIMEOUT_FACTOR) {
        char rsp[1024];
        dn_sprintf(rsp, "Timeout factor: %d\n", g_timeout_factor);
        return stats_http_rsp(sd, rsp, dn_strlen(rsp));
    } else if (cmd == CMD_SET_TIMEOUT_FACTOR) {
        int8_t timeout_factor = 0;
        log_warn("st_cmd.req_data '%.*s' ", st_cmd.req_data);
        sscanf(st_cmd.req_data.data, "%d", &timeout_factor);
        log_warn("timeout factor = %d", timeout_factor);
        // make sure timeout factor is within a range
        if (timeout_factor < 1)
            timeout_factor = 1;
        if (timeout_factor > 10)
            timeout_factor = 10;
        g_timeout_factor = timeout_factor;
        log_warn("setting timeout_factor to %d", g_timeout_factor);
        return stats_http_rsp(sd, ok.data, ok.len);
    } else if (cmd == CMD_PEER_DOWN || cmd == CMD_PEER_UP || cmd == CMD_PEER_RESET) {
        log_debug(LOG_VERB, "st_cmd.req_data '%.*s' ", st_cmd.req_data);
        struct server_pool *sp = &st->ctx->pool;
        uint32_t i, len;

        //I think it is ok to keep this simple without a synchronization
        for (i = 0, len = array_n(&sp->peers); i < len; i++) {
            struct node *peer = array_get(&sp->peers, i);
            log_debug(LOG_VERB, "peer '%.*s' ", peer->name);

            if (string_compare(&st_cmd.req_data, &all) == 0) {
                log_debug(LOG_VERB, "\t\tSetting peer '%.*s' to state %d due to RESET/ALL command", st_cmd.req_data, cmd);
                peer->state = RESET;
            } else if (string_compare(&peer->name, &st_cmd.req_data) == 0) {
                log_debug(LOG_VERB, "\t\tSetting peer '%.*s' to a new state due to command %d", st_cmd.req_data, cmd);
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

    THROW_STATUS(stats_listen(st));

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
stats_create(uint16_t stats_port, struct string pname, int stats_interval,
             char *source, struct server_pool *sp, struct context *ctx)
{
    rstatus_t status;
    struct stats *st;

    struct string stats_ip;
    get_host_from_pname(&stats_ip, &pname);


    st = dn_alloc(sizeof(*st));
    if (st == NULL) {
        return NULL;
    }

    st->port = stats_port;
    st->interval = stats_interval;
    string_init(&st->addr);
    if (string_duplicate(&st->addr,&stats_ip) != DN_OK) {
    	goto error;
    }

    st->start_ts = (int64_t)time(NULL);

    st->buf.len = 0;
    st->buf.data = NULL;
    st->buf.size = 0;

    st->tid = (pthread_t) -1;
    st->sd = -1;

    string_set_text(&st->service_str, "service");
    string_set_text(&st->service, "dynomite");

    string_set_text(&st->source_str, "source");
    string_set_raw(&st->source, source);

    string_set_text(&st->version_str, "version");
    string_set_text(&st->version, VERSION);

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

    // cross region average latency
    string_set_text(&st->cross_region_avg_rtt, "average_cross_region_rtt");
    string_set_text(&st->cross_region_99_rtt, "99_cross_region_rtt");

    string_set_text(&st->client_out_queue_99, "client_out_queue_99");
    string_set_text(&st->server_in_queue_99, "server_in_queue_99");
    string_set_text(&st->server_out_queue_99, "server_out_queue_99");
    string_set_text(&st->dnode_client_out_queue_99, "dnode_client_out_queue_99");
    string_set_text(&st->peer_in_queue_99, "peer_in_queue_99");
    string_set_text(&st->peer_out_queue_99, "peer_out_queue_99");
    string_set_text(&st->remote_peer_in_queue_99, "remote_peer_in_queue_99");
    string_set_text(&st->remote_peer_out_queue_99, "remote_peer_out_queue_99");

    string_set_text(&st->alloc_msgs_str, "alloc_msgs");
    string_set_text(&st->free_msgs_str, "free_msgs");
    string_set_text(&st->alloc_mbufs_str, "alloc_mbufs");
    string_set_text(&st->free_mbufs_str, "free_mbufs");
    string_set_text(&st->dyn_memory_str, "dyn_memory");

    string_set_text(&st->rack_str, "rack");

    string_copy(&st->rack, sp->rack.data, sp->rack.len);

    string_set_text(&st->dc_str, "dc");
    string_copy(&st->dc, sp->dc.data, sp->dc.len);

    st->updated = 0;
    st->aggregate = 0;

    histo_init(&st->latency_histo);
    histo_init(&st->payload_size_histo);

    histo_init(&st->server_latency_histo);
    histo_init(&st->cross_zone_latency_histo);
    histo_init(&st->cross_region_latency_histo);

    histo_init(&st->server_queue_wait_time_histo);
    histo_init(&st->cross_zone_queue_wait_time_histo);
    histo_init(&st->cross_region_queue_wait_time_histo);

    histo_init(&st->client_out_queue);
    histo_init(&st->server_in_queue);
    histo_init(&st->server_out_queue);
    histo_init(&st->dnode_client_out_queue);
    histo_init(&st->peer_in_queue);
    histo_init(&st->peer_out_queue);
    histo_init(&st->remote_peer_in_queue);
    histo_init(&st->remote_peer_out_queue);
    st->reset_histogram = 0;
    st->alloc_msgs = 0;
    st->free_msgs = 0;
    st->alloc_mbufs = 0;
    st->free_mbufs = 0;
    st->dyn_memory = 0;

    /* map server pool to current (a), shadow (b) and sum (c) */

    status = stats_pool_init(&st->current, sp);
    if (status != DN_OK) {
        goto error;
    }

    status = stats_pool_init(&st->shadow, sp);
    if (status != DN_OK) {
        goto error;
    }

    status = stats_pool_init(&st->sum, sp);
    if (status != DN_OK) {
        goto error;
    }

    status = stats_create_bufs(st);
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
    stats_destroy_buf(&st->buf);
    stats_destroy_buf(&st->clus_desc_buf);
    dn_free(st);
}

void
stats_swap(struct stats *st)
{
    struct rusage r_usage;
    if (!stats_enabled) {
        return;
    }

    if (st->aggregate == 1) {
        log_debug(LOG_PVERB, "skip swap of current %p shadow %p as aggregator "
                  "is busy", &st->current, &st->shadow);
        return;
    }

    if (st->updated == 0) {
        log_debug(LOG_PVERB, "skip swap of current %p shadow %p as there is "
                  "nothing new", &st->current, &st->shadow);
        return;
    }

    log_debug(LOG_PVERB, "swap stats current %p shadow %p", &st->current,
              &st->shadow);


    //set the latencies
    histo_compute(&st->latency_histo);

    histo_compute(&st->payload_size_histo);

    histo_compute(&st->server_latency_histo);
    histo_compute(&st->cross_zone_latency_histo);
    histo_compute(&st->cross_region_latency_histo);

    histo_compute(&st->server_queue_wait_time_histo);
    histo_compute(&st->cross_zone_queue_wait_time_histo);
    histo_compute(&st->cross_region_queue_wait_time_histo);

    histo_compute(&st->client_out_queue);
    histo_compute(&st->server_in_queue);
    histo_compute(&st->server_out_queue);
    histo_compute(&st->dnode_client_out_queue);
    histo_compute(&st->peer_in_queue);
    histo_compute(&st->peer_out_queue);
    histo_compute(&st->remote_peer_in_queue);
    histo_compute(&st->remote_peer_out_queue);

    st->alloc_msgs = msg_alloc_msgs();
    st->free_msgs = msg_free_queue_size();
    st->alloc_mbufs = mbuf_alloc_get_count();
    st->free_mbufs = mbuf_free_queue_size();

    getrusage(RUSAGE_SELF,&r_usage);
    st->dyn_memory  = r_usage.ru_maxrss;

    // swap current and shadow
    struct stats_pool temp = st->current;
    st->current = st->shadow;
    st->shadow = temp;

    /*
     * Reset current (a) stats before giving it back to generator to keep
     * stats addition idempotent
     */
    stats_pool_reset(&st->current);
    st->updated = 0;

    st->aggregate = 1;

}

uint64_t
_stats_pool_get_ts(struct context *ctx,
                   stats_pool_field_t fidx)
{
   struct stats *st = ctx->stats;
   struct stats_pool *stp = &st->current;
   struct stats_metric *stm = array_get(&stp->metric, fidx);
   return stm->value.counter;
}

int64_t
_stats_pool_get_val(struct context *ctx,
                    stats_pool_field_t fidx)
{
    struct stats *st = ctx->stats;
    struct stats_pool *stp = &st->current;
    struct stats_metric *stm = array_get(&stp->metric, fidx);
    return stm->value.counter;
}


static struct stats_metric *
stats_pool_to_metric(struct context *ctx,
                     stats_pool_field_t fidx)
{
    struct stats *st = ctx->stats;
    struct stats_pool *stp = &st->current;
    struct stats_metric *stm = array_get(&stp->metric, fidx);
    st->updated = 1;
    return stm;
}

void
_stats_pool_incr(struct context *ctx,
                 stats_pool_field_t fidx)
{
    struct stats_metric *stm = stats_pool_to_metric(ctx, fidx);

    ASSERT(stm->type == STATS_COUNTER || stm->type == STATS_GAUGE);
    stm->value.counter++;

    log_debug(LOG_VVVERB, "incr field '%.*s' to %"PRId64"", stm->name.len,
              stm->name.data, stm->value.counter);
}

void
_stats_pool_decr(struct context *ctx,
                 stats_pool_field_t fidx)
{
    struct stats_metric *stm = stats_pool_to_metric(ctx, fidx);

    ASSERT(stm->type == STATS_GAUGE);
    stm->value.counter--;

    log_debug(LOG_VVVERB, "decr field '%.*s' to %"PRId64"", stm->name.len,
              stm->name.data, stm->value.counter);
}

void
_stats_pool_incr_by(struct context *ctx,
                    stats_pool_field_t fidx, int64_t val)
{
    struct stats_metric *stm = stats_pool_to_metric(ctx, fidx);

    ASSERT(stm->type == STATS_COUNTER || stm->type == STATS_GAUGE);
    stm->value.counter += val;

    log_debug(LOG_VVVERB, "incr by field '%.*s' to %"PRId64"", stm->name.len,
              stm->name.data, stm->value.counter);
}

void
_stats_pool_decr_by(struct context *ctx,
                    stats_pool_field_t fidx, int64_t val)
{
    struct stats_metric *stm = stats_pool_to_metric(ctx, fidx);

    ASSERT(stm->type == STATS_GAUGE);
    stm->value.counter -= val;

    log_debug(LOG_VVVERB, "decr by field '%.*s' to %"PRId64"", stm->name.len,
              stm->name.data, stm->value.counter);
}

void
_stats_pool_set_ts(struct context *ctx,
                   stats_pool_field_t fidx, int64_t val)
{
    struct stats_metric *stm = stats_pool_to_metric(ctx, fidx);

    ASSERT(stm->type == STATS_TIMESTAMP);
    stm->value.timestamp = val;

    log_debug(LOG_VVVERB, "set ts field '%.*s' to %"PRId64"", stm->name.len,
              stm->name.data, stm->value.timestamp);
}

uint64_t
_stats_server_get_ts(struct context *ctx,
                     stats_server_field_t fidx)
{
   struct stats *st = ctx->stats;
   struct stats_pool *stp = &st->current;
   struct stats_server *sts = &stp->server;
   struct stats_metric *stm = array_get(&sts->metric, fidx);

   return stm->value.timestamp;
}

void
_stats_pool_set_val(struct context *ctx,
                    stats_pool_field_t fidx, int64_t val)
{
   struct stats_metric *stm = stats_pool_to_metric(ctx, fidx);

   stm->value.counter = val;

   log_debug(LOG_VVVERB, "set val field '%.*s' to %"PRId64"", stm->name.len,
             stm->name.data, stm->value.counter);
}

int64_t
_stats_server_get_val(struct context *ctx,
                      stats_server_field_t fidx)
{
   struct stats *st = ctx->stats;
   struct stats_pool *stp = &st->current;
   struct stats_server *sts = &stp->server;
   struct stats_metric *stm = array_get(&sts->metric, fidx);

   return stm->value.counter;
}

static struct stats_metric *
stats_server_to_metric(struct context *ctx,
                       stats_server_field_t fidx)
{
   struct stats *st = ctx->stats;
   struct stats_pool *stp = &st->current;
   struct stats_server *sts = &stp->server;
   struct stats_metric *stm = array_get(&sts->metric, fidx);

    st->updated = 1;

    log_debug(LOG_VVVERB, "metric '%.*s' for server",
              stm->name.len, stm->name.data);

    return stm;
}

void
_stats_server_incr(struct context *ctx,
                   stats_server_field_t fidx)
{
    
    struct stats_metric *stm = stats_server_to_metric(ctx, fidx);

    ASSERT(stm->type == STATS_COUNTER || stm->type == STATS_GAUGE);
    stm->value.counter++;

    log_debug(LOG_VVVERB, "incr field '%.*s' to %"PRId64"", stm->name.len,
              stm->name.data, stm->value.counter);
    
}

void
_stats_server_decr(struct context *ctx,
                   stats_server_field_t fidx)
{

    struct stats_metric *stm = stats_server_to_metric(ctx, fidx);

    ASSERT(stm->type == STATS_GAUGE);
    stm->value.counter--;

    log_debug(LOG_VVVERB, "decr field '%.*s' to %"PRId64"", stm->name.len,
              stm->name.data, stm->value.counter);

}

void
_stats_server_incr_by(struct context *ctx,
                      stats_server_field_t fidx, int64_t val)
{

    struct stats_metric *stm = stats_server_to_metric(ctx, fidx);

    ASSERT(stm->type == STATS_COUNTER || stm->type == STATS_GAUGE);
    stm->value.counter += val;

    log_debug(LOG_VVVERB, "incr by field '%.*s' to %"PRId64"", stm->name.len,
              stm->name.data, stm->value.counter);

}

void
_stats_server_decr_by(struct context *ctx,
                      stats_server_field_t fidx, int64_t val)
{

    struct stats_metric *stm = stats_server_to_metric(ctx, fidx);

    ASSERT(stm->type == STATS_GAUGE);
    stm->value.counter -= val;

    log_debug(LOG_VVVERB, "decr by field '%.*s' to %"PRId64"", stm->name.len,
              stm->name.data, stm->value.counter);

}

void
_stats_server_set_ts(struct context *ctx,
                     stats_server_field_t fidx, uint64_t val)
{

    struct stats_metric *stm = stats_server_to_metric(ctx, fidx);

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
