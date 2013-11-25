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
parse_int(uint32_t *start, uint32_t *end)
{
    /*     int result = Character.digit(source[start++], 10); */
    /*     if (result == -1) */
    /*         throw new NumberFormatException(new String(source)); */

    /*     for (int index = start; index<end; index++) { */
    /*         int nextVal = Character.digit(source[index], 10); */
    /*         if (nextVal == -1) */
    /*             throw new NumberFormatException(new String(source)); */
    /*         result = 10*result + nextVal; */
    /*     } */

    /*     return result; */
    /* } */

    return 0;
}


rstatus_t 
set_dyn_token(uint32_t *start, uint32_t *end, struct dyn_token *token)
{
    ASSERT(start < end);
    ASSERT(token != NULL);

    /* TODO-jeb: check for whitespace */

    uint32_t *p = start;
    uint32_t *q = end;
    if (p[0] == (uint32_t)'-') {
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
        uint32_t local_int = parse_int(p, DIGITS_PER_INT);
        /* destructiveMulAdd(buf, intRadix[10], local_int);  */
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


