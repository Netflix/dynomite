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
#include <nc_util.h>

/* bitsPerDigit radix of 10 multiplied by 1024, rounded up to avoid underallocation */
#define BITS_PER_DIGIT 3402

/* TODO: jeb - this should be 9, but hacked for POC */
#define DIGITS_PER_INT 10

void 
init_dyn_token(struct dyn_token *token)
{
    token->signum = 0;
    token->mag = NULL;
    token->len = 0;
}

void 
deinit_dyn_token(struct dyn_token *token)
{
    nc_free(token->mag);
    token->signum = 0;
    token->len = 0;
}

rstatus_t 
size_dyn_token(struct dyn_token *token, uint32_t token_len)
{
    uint32_t size = sizeof(uint32_t) * token_len; 
    token->mag = nc_alloc(size);
    if (token->mag == NULL) {
        return NC_ENOMEM;
    }
    memset(token->mag, 0, size);
    token->len = token_len;
    token->signum = 0;

    return NC_OK;
}

void 
set_int_dyn_token(struct dyn_token *token, uint32_t val)
{
    token->mag[0] = val;
    token->len = 1;
    token->signum = val > 0 ? 1 : 0;
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
parse_dyn_token(uint8_t *start, uint32_t len, struct dyn_token *token)
{
    ASSERT(len > 0);
    ASSERT(token != NULL);

    /* TODO-jeb: check for whitespace */
    char sign = '-';
    uint8_t *p = start;
    uint8_t *q = p + len;
    uint32_t digits = len;
    if (p[0] == sign) {
        token->signum = -1;
        p++;
        digits--;
        ASSERT(digits > 0);
    } else if (digits == 1 && p[0] == '0') {
        token->signum = 0;
    } else {
        token->signum = 1;
    }

    int nwords;
    /* if (digits < 10) { */
        nwords = 1;
    /* } else { */
    /*     uint32_t nbits = ((digits * BITS_PER_DIGIT) >> 10) + 1; */
    /*     nwords = (nbits + 32) >> 5; */
    /* } */

    token->mag = nc_alloc(nwords * sizeof(uint32_t));
    if (token->mag == NULL) {
        return NC_ENOMEM;
    }
    memset(token->mag, 0, nwords * sizeof(uint32_t));
    uint32_t *buf = token->mag;
    token->len = nwords;

    // Process first (potentially short) digit group
    uint32_t first_group_len = digits % DIGITS_PER_INT;
    if (first_group_len == 0)
        first_group_len = DIGITS_PER_INT;
    buf[nwords - 1] = nc_atoui(p, first_group_len);
    p += first_group_len;
    
    // Process remaining digit groups
    while (p < q) {
        uint32_t local_int = nc_atoui(p, DIGITS_PER_INT);
        add_next_word(buf, nwords, local_int);
        p += DIGITS_PER_INT;
    }

    return NC_OK;
}

int32_t 
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


