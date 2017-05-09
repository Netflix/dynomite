#pragma once

#include "dyn_connection.h"
#include "dyn_types.h"
//struct conn_pool;
typedef struct conn_pool conn_pool_t;

/**
 * Creates a connection pool with max_connections in it.
 * creates connection objects using conn_get and uses func_conn_init to initialize
 * them.
 */
conn_pool_t *conn_pool_create(struct context *ctx, void *owner,
                              uint8_t max_connections,
                              func_conn_init_t func_conn_init);

/**
 * This function starts a preconnect process for every underlying connection object
 * but does not wait for it to finish. The conn_connect function automatically
 * adds the connection to the event loop
 */
rstatus_t conn_pool_preconnect(conn_pool_t *cp);

/**
 * Given a tag (just a uint16_t number), get a connection from the connection pool.
 * The purpose of the tag is to get the same underlying connection for a given tag.
 * If the tag is not seen before, a new random connection is allocated.
 * And all subsequent conn_pool_get with the same tag should yield the same
 * underlying connection. The tag could be as simple as the socket number of the
 * client connection to map to the underlying resource in the connection pool.
 * If this association is missing then the request from the client connections
 * will not follow strict ordering leading to out of order execution on differnt
 * nodes
 */
struct conn *conn_pool_get(conn_pool_t *cp, uint16_t tag);

/**
 * This function, tears down all the connection in the pool, clears up its state
 * 
 */
rstatus_t conn_pool_reset(conn_pool_t *cp);

/**
 * If a connection that is part of a pool is being closed, this function should
 * called so the pool can do its cleanup.
 * 
 */
void conn_pool_notify_conn_close(conn_pool_t *cp, struct conn *conn);
