#pragma once
#include <dyn_error.h>
//#include <dyn_thread_ctx.h>
#include <event/dyn_event.h>

/* This is the thread IPC interface.
 * Can be implemented by either message queues or unix domain sockets
 * provided it works with epoll */

typedef struct thread_ipc *pthread_ipc;
struct thread_ctx;
struct thread_ipc {

    // The pollable interface to be used with event notification system
    struct pollable p;

    // Initialize this ipc
    rstatus_t (*init)(pthread_ipc ptipc, struct thread_ctx *owner);

    // Open this ipc for reading
    rstatus_t (*open_to_read)(pthread_ipc ptipc);

    // Open this ipc for writing
    rstatus_t (*open_to_write)(pthread_ipc ptipc);

    // receive next message from this ipc
    struct msg *(*receive)(pthread_ipc ptipc);

    // send message to this ipc
    rstatus_t (*send)(pthread_ipc ptipc, struct msg *msg);

    // destroy this ipc
    rstatus_t (*destroy)(pthread_ipc ptipc);

    // Owner ptctx, this guy for whom the messages will be sent
    struct thread_ctx *owner_ptctx;
};

pthread_ipc thread_ipc_mq_create(void);

static inline rstatus_t
thread_ipc_init(pthread_ipc ptipc, struct thread_ctx *owner) {
    return ptipc->init(ptipc, owner);
}

static inline struct msg *
thread_ipc_receive(pthread_ipc ptipc)
{
    return ptipc->receive(ptipc);
}

static inline rstatus_t
thread_ipc_send(pthread_ipc ptipc, struct msg *msg)
{
    return ptipc->send(ptipc, msg);
}

static inline rstatus_t
thread_ipc_destroy(pthread_ipc ptipc)
{
    return ptipc->destroy(ptipc);
}
