/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 


#ifndef _DYN_TOKEN_H_
#define _DYN_TOKEN_H_

#include <dyn_core.h>

struct dyn_token {
    uint32_t signum;
    uint32_t *mag;
    uint32_t len;
};

void init_dyn_token(struct dyn_token *token);
void deinit_dyn_token(struct dyn_token *token);
rstatus_t size_dyn_token(struct dyn_token *token, uint32_t size);

/**
 * convenience function for setting a token whose value is just an int
 */
void set_int_dyn_token(struct dyn_token *token, uint32_t val);

rstatus_t parse_dyn_token(uint8_t *start, uint32_t len, struct dyn_token *token);
int32_t cmp_dyn_token(struct dyn_token *t1, struct dyn_token *t2);

#endif
