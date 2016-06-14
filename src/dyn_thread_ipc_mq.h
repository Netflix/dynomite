#pragma once
#include <dyn_error.h>
#include <dyn_thread_ipc.h>
#include <mqueue.h>

typedef struct thread_ipc_mq {
    struct thread_ipc ops;
    struct string name;
}*pthread_ipc_mq;

