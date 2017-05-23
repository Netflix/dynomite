/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 


#include "dyn_core.h"

#ifndef _DYN_TOKEN_H_
#define _DYN_TOKEN_H_


struct dyn_token {
    uint32_t signum;
    uint32_t *mag;
    uint32_t len;
};


void init_dyn_token(struct dyn_token *token);
void deinit_dyn_token(struct dyn_token *token);
rstatus_t size_dyn_token(struct dyn_token *token, uint32_t size);
rstatus_t copy_dyn_token(const struct dyn_token * src, struct dyn_token * dst);

/**
 * convenience function for setting a token whose value is just an int
 */
void set_int_dyn_token(struct dyn_token *token, uint32_t val);

rstatus_t parse_dyn_token(uint8_t *start, uint32_t len, struct dyn_token *token);
int32_t cmp_dyn_token(struct dyn_token *t1, struct dyn_token *t2);
rstatus_t derive_tokens(struct array *tokens, uint8_t *start, uint8_t *end);
rstatus_t derive_token(struct dyn_token *token, uint8_t *start, uint8_t *end);
void print_dyn_token(struct dyn_token *token, int num_tabs);

#endif
