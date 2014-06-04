/*
 * dyn_ring_queue.c
 *
 *  Created on: May 31, 2014
 *      Author: mdo
 */

#include "dyn_core.h"
#include "dyn_ring_queue.h"
#include "hashkit/dyn_token.h"


struct ring_message * create_ring_message()
{
	struct ring_message *result = nc_alloc(sizeof(*result));
	ring_message_init(result);

	return result;
}

rstatus_t ring_message_init(struct ring_message *msg)
{
	if (msg == NULL)
		return NC_ERROR;

	msg->node = nc_alloc(sizeof(struct node));

	return NC_OK;
}


rstatus_t ring_message_deinit(struct ring_message *msg)
{
	if (msg == NULL)
		return NC_ERROR;

    nc_free(msg->node);
    nc_free(msg->cb);

    nc_free(msg);

    return NC_OK;
}



struct node * create_node()
{
    struct node *result = nc_alloc(sizeof(*result));
    node_init(result);

    return result;
}


rstatus_t node_init(struct node *node)
{
     if (node == NULL)
    	 return NC_ERROR;

     array_init(&node->tokens, 1, sizeof(struct dyn_token));
     string_init(&node->name);
     string_init(&node->pname);

     node->port = 0;

     node->next_retry = 0;
     node->last_retry = 0;
     node->failure_count = 0;

     node->is_seed = false;
     node->is_local = false;
     node->status = 2;

     return NC_OK;
}


rstatus_t node_deinit(struct node *node)
{
     if (node == NULL)
    	 return NC_ERROR;

     array_deinit(&node->tokens);
     string_deinit(&node->name);
     string_deinit(&node->pname);

     nc_free(node);

     return NC_OK;
}
