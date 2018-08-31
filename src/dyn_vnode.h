#pragma once
#include <dyn_types.h>

rstatus_t vnode_update(struct server_pool *pool);
uint32_t vnode_dispatch(struct continuum *continuum, uint32_t ncontinuum,
                        struct dyn_token *token);
