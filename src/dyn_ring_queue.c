/*
 * dyn_ring_queue.c
 *
 *  Created on: May 31, 2014
 *      Author: mdo
 */

#include "dyn_array.h"
#include "dyn_core.h"
#include "dyn_gossip.h"
#include "dyn_ring_queue.h"
#include "dyn_token.h"


//should use pooling to store struct ring_message so that we can reuse
struct ring_msg *
create_ring_msg(void)
{
	struct ring_msg *msg = dn_alloc(sizeof(*msg));

	if (msg == NULL)
		return NULL;

	ring_msg_init(msg, 1, true);
    msg->data = NULL;

	return msg;
}


struct ring_msg *
create_ring_msg_with_data(int capacity)
{
	struct ring_msg *msg = dn_alloc(sizeof(*msg));

	if (msg == NULL)
		return NULL;

	ring_msg_init(msg, 1, true);

	msg->data = dn_zalloc(sizeof(uint8_t) * capacity);
	msg->capacity = capacity;
    msg->len = 0;

	return msg;
}


struct ring_msg *
create_ring_msg_with_size(uint32_t size, bool init_node)
{
	struct ring_msg *msg = dn_alloc(sizeof(*msg));

	if (msg == NULL)
		return NULL;

	ring_msg_init(msg, size, init_node);
	msg->data = NULL;
	msg->capacity = 0;
	msg->len = 0;

	return msg;
}


rstatus_t
ring_msg_init(struct ring_msg *msg, uint32_t n, bool init_node)
{
	if (msg == NULL)
		return DN_ERROR;

	array_init(&msg->nodes, n, sizeof(struct node));
	if (init_node) {
		uint32_t i;
		for(i=0; i<n; i++) {
			struct node *node = array_push(&msg->nodes);
			node_init(node);
		}
	}

	//msg->node = dn_alloc(sizeof(struct node));
	//msg->node = create_node();


	return DN_OK;
}


rstatus_t
ring_msg_deinit(struct ring_msg *msg)
{
	if (msg == NULL)
		return DN_ERROR;

	uint32_t i;
	for(i=0; i<array_n(&msg->nodes); i++) {
		struct node *node = array_get(&msg->nodes, i);
		node_deinit(node);
	}

	if (msg->data != NULL) {
		dn_free(msg->data);
	}

	array_deinit(&msg->nodes);
	dn_free(msg);

	return DN_OK;
}



struct node *
create_node()
{
	struct node *result = dn_alloc(sizeof(*result));
	node_init(result);

	return result;
}


rstatus_t
node_init(struct node *node)
{
	if (node == NULL)
		return DN_ERROR;

	init_dyn_token(&node->token);
	string_init(&node->dc);
	string_init(&node->rack);
	string_init(&node->name);
	string_init(&node->pname);

	node->port = 8101;

	node->next_retry = 0;
	node->last_retry = 0;
	node->failure_count = 0;

	node->is_seed = false;
	node->is_local = false;
	node->state = INIT;

	return DN_OK;
}


rstatus_t
node_deinit(struct node *node)
{
	if (node == NULL)
		return DN_ERROR;

	//array_deinit(&node->tokens);
	deinit_dyn_token(&node->token);
	string_deinit(&node->dc);
	string_deinit(&node->rack);
	string_deinit(&node->name);
	string_deinit(&node->pname);

	//dn_free(node);

	return DN_OK;
}


rstatus_t
node_copy(const struct node *src, struct node *dst)
{
	if (src == NULL || dst == NULL)
		return DN_ERROR;

	dst->state = src->state;
	dst->is_local = src->is_local;
	dst->is_seed = src->is_seed;
	dst->failure_count = src->failure_count;
	dst->last_retry = src->last_retry;
	dst->next_retry = src->next_retry;
	dst->port = src->port;


	string_copy(&dst->pname, src->pname.data, src->pname.len);
	string_copy(&dst->name, src->name.data, src->name.len);
	string_copy(&dst->rack, src->rack.data, src->rack.len);
	string_copy(&dst->dc, src->dc.data, src->dc.len);


	copy_dyn_token(&src->token, &dst->token);
    return DN_OK;
}
