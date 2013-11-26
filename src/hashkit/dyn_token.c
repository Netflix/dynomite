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

#include <stdint.h>
#include <stddef.h>
#include <dyn_token.h>

#define BITS_PER_DIGIT 3402
#define DIGITS_PER_INT 9

void 
init_dyn_token(struct dyn_token *token)
{
    token->signum = 0;
    token->mag = NULL;
    token->len = 0;
}

static uint32_t 
parse_int(uint8_t *start, uint8_t *end)
{
    ASSERT (start < end);

    uint8_t res = start[0];
    ASSERT (res >= '0' && res <= '9');

    uint8_t len = end - start;
    for (uint8_t i = 1; i < len; i++) {
        uint8_t next_val = start[i];
        ASSERT (res >= '0' && res <= '9');
        res = 10 * res + next_val;
    }

    return res;
}

static void
add_next_word(uint32_t *buf, uint32_t len, uint32_t next_int)
{
    uint64_t product = 0;
    uint64_t carry = 0;

    /* magick! */
    uint32_t radix_val = 0x17179149;

    for (int i = len - 1; i >= 0; i--) {
        product = radix_val * buf[i] + carry;
        buf[i] = (uint32_t)product;
        carry = product >> 32;
    }

    uint64_t sum = buf[len-1] + next_int;
    buf[len-1] = (uint32_t)sum;
    carry = sum >> 32;
    for (int i = len-2; i >= 0; i--) {
        sum = buf[i] + carry;
        buf[i] = (uint32_t)sum;
        carry = sum >> 32;
    }
}

rstatus_t 
set_dyn_token(uint8_t *start, uint8_t *end, struct dyn_token *token)
{
    ASSERT(start < end);
    ASSERT(token != NULL);

    /* TODO-jeb: check for whitespace */
    char delim = '=';
    uint8_t *p = start;
    uint8_t *q = end;
    if (p[0] == delim) {
        token->signum = -1;
        p++;
        ASSERT(p < q);
    }

    uint32_t digits = q - p;
    int nwords;
    if (digits < 10) {
        nwords = 1;
    } else {
        uint32_t nbits = (int)(((digits * BITS_PER_DIGIT) >> 10) + 1);
        nwords = (nbits + 32) >> 5;
    }
    uint32_t buf_len = 8;
    uint32_t buf[8] = {0, 0, 0, 0, 0, 0, 0, 0};

    // Process first (potentially short) digit group
    uint32_t first_group_len = digits % DIGITS_PER_INT;
    if (first_group_len == 0)
        first_group_len = DIGITS_PER_INT;
    buf[buf_len - 1] = parse_int(p, p + first_group_len);
    
    // Process remaining digit groups
    while (p < q) {
        uint32_t local_int = parse_int(p, p + ((uint8_t)DIGITS_PER_INT));
        add_next_word(buf, buf_len, local_int);
    }

    token->mag = nc_alloc(nwords * sizeof(uint32_t));
    if (token->mag == NULL) {
        return NC_ENOMEM;
    }
    memcpy(token->mag, buf, nwords);
    token->len = nwords;

    return NC_OK;
}

uint32_t 
cmp_dyn_token(struct dyn_token *t1, struct dyn_token *t2)
{
    ASSERT(t1 != NULL);
    ASSERT(t2 != NULL);
 
   if (t1->signum == t2->signum) {
        if (t1->signum == 0) {
            return 0;
        }

        if (t1-> len == t2->len) {
            for (int i = 0; i < t1->len; i++) {
                uint32_t a = t1->mag[i];
                uint32_t b = t2->mag[i];
                if (a != b) {
                    return a > b ? 1 : -1;
                }
            }
            return 0;
        }
        
        return t1->len > t2->len ? 1 : -1;
    }

    return t1->signum > t2->signum ? 1 : -1;
}


