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

#ifndef _DYN_MBUF_H_
#define _DYN_MBUF_H_



typedef void (*func_mbuf_copy_t)(struct mbuf *, void *);

struct mbuf {
    uint32_t           magic;   /* mbuf magic (const) */
    STAILQ_ENTRY(mbuf) next;    /* next mbuf */
    uint8_t            *pos;    /* read marker */
    uint8_t            *last;   /* write marker */
    uint8_t            *start;  /* start of buffer (const) */
    uint8_t            *end;    /* end of buffer (const) */
    uint8_t            *end_extra; /*end of the buffer - including the extra region */
    uint32_t           read_flip; /* readable flag used in encryption/decryption mode */
    uint32_t           chunk_size;
};

STAILQ_HEAD(mhdr, mbuf);

#define MBUF_MAGIC      0xdeadbeef
#define MBUF_MIN_SIZE   512
#define MBUF_MAX_SIZE   512000
#define MBUF_SIZE       16384
#define MBUF_HSIZE      sizeof(struct mbuf)
#define MBUF_ESIZE      16


static inline bool
mbuf_empty(struct mbuf *mbuf)
{
    return mbuf->pos == mbuf->last ? true : false;
}

static inline bool
mbuf_full(struct mbuf *mbuf)
{
    return mbuf->last == mbuf->end? true : false;
}

void mbuf_init(struct instance *nci);
void mbuf_deinit(void);
struct mbuf *mbuf_get(void);
void mbuf_put(struct mbuf *mbuf);
uint64_t mbuf_alloc_get_count(void);
uint64_t mbuf_free_queue_size(void);
void mbuf_dump(struct mbuf *mbuf);
void mbuf_rewind(struct mbuf *mbuf);
uint32_t mbuf_length(struct mbuf *mbuf);
uint32_t mbuf_size(struct mbuf *mbuf);
size_t mbuf_data_size(void);
void mbuf_insert(struct mhdr *mhdr, struct mbuf *mbuf);
void mbuf_insert_head(struct mhdr *mhdr, struct mbuf *mbuf);
void mbuf_insert_after(struct mhdr *mhdr, struct mbuf *mbuf, struct mbuf *nbuf);
void mbuf_remove(struct mhdr *mhdr, struct mbuf *mbuf);
void mbuf_copy(struct mbuf *mbuf, uint8_t *pos, size_t n);
struct mbuf *mbuf_split(struct mhdr *h, uint8_t *pos, func_mbuf_copy_t cb, void *cbarg);

void mbuf_write_char(struct mbuf *mbuf, char ch);
void mbuf_write_string(struct mbuf *mbuf,  struct string *s);
void mbuf_write_uint8(struct mbuf *mbuf, uint8_t num);
void mbuf_write_uint32(struct mbuf *mbuf, uint32_t num);
void mbuf_write_uint64(struct mbuf *mbuf, uint64_t num);
void mbuf_write_mbuf(struct mbuf *mbuf, struct mbuf *data);
void mbuf_write_bytes(struct mbuf *mbuf, char *data, int len);

struct mbuf *mbuf_alloc(size_t size);
void mbuf_dealloc(struct mbuf *mbuf);

#endif
