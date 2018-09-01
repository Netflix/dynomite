#include "dyn_task.h"

#include <stdbool.h>

#include "dyn_util.h"

/**
 * This is a generic task manager. There was a increasing demand in Dynomite to
 * create a module to schedule task a specific times. For example reconnecting
 * a disconnected peer, or handling gossip messages, or timeouts etc. This is a module to
 * implement such use cases.
 *
 * Sequence of calls:
 *
 * task_mgr_init()
 * schedule_task()
 * execute_expired_tasks()
 *
 * The time complexity for insert/delete in the red black tree is O(log N), where
 * N is the number of elements in the tree.
 *
 */



struct rbtree task_rbt; /* rbtree which holds the tasks */
struct rbnode task_rbs; /* rbtree sentinel */

// Individual task
struct task {
    struct rbnode rbnode; /* always be the first field */
    task_handler_1  handler;
    void *arg1;
};

rstatus_t
task_mgr_init()
{
    rbtree_init(&task_rbt, &task_rbs);
    return DN_OK;
}

static struct task *
_create_task(void)
{
    struct task *task = dn_alloc(sizeof(struct task));
    if (!task)
        return NULL;
    memset(task, 0, sizeof(struct task));
    return task;
}

struct task *
schedule_task_1(task_handler_1 handler1, void *arg1, msec_t timeout)
{
    struct task *task = _create_task();
    task->handler = handler1;
    task->arg1 = arg1;

    msec_t now_ms = dn_msec_now();

    struct rbnode *rbnode = (struct rbnode *)task;
    rbnode->timeout = timeout;
    rbnode->key = now_ms + timeout;
    rbnode->data = task;
    rbtree_insert(&task_rbt, rbnode);
    return task;
}


msec_t
time_to_next_task(void)
{
    struct rbnode *rbnode = rbtree_min(&task_rbt);
    if (!rbnode)
        return UINT64_MAX;
    msec_t now_ms = dn_msec_now();
    msec_t fire_at_ms = rbnode->key;
    if (now_ms > fire_at_ms)
        return 0;
    return fire_at_ms - now_ms;
}

static bool
task_expired(struct task *task)
{
    msec_t now_ms = dn_msec_now();
    msec_t fire_at_ms = task->rbnode.key;

    if (now_ms > fire_at_ms)
        return true;
    return false;
}

void
execute_expired_tasks(uint32_t limit)
{
    uint32_t executed = 0;
    for (;;) {
        struct rbnode *rbnode = rbtree_min(&task_rbt);
        if (!rbnode) {
            return;
        }

        struct task *task = rbnode->data;

        if (task_expired(task)) {
            rbtree_delete(&task_rbt, rbnode);
            task->handler(task->arg1);
            dn_free(task);
            executed++;
            if (!limit && executed == limit)
                return;
            continue;
        }
        break;
    }
}

void
cancel_task(struct task *task)
{
    struct rbnode *rbnode = (struct rbnode *)task;
    rbtree_delete(&task_rbt, rbnode);
    dn_free(task);
}
