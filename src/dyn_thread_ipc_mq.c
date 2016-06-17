#include <dyn_thread_ctx.h>
#include <dyn_thread_ipc_mq.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>


static inline pthread_ipc
get_thread_ipc(pthread_ipc_mq ipc)
{
    return &ipc->ops;
}

static inline struct pollable * 
get_pollable(pthread_ipc_mq ipc)
{
    return &ipc->ops.p;
}

static rstatus_t
mq_ipc_init(pthread_ipc ptipc, pthread_ctx ptctx)
{
    pthread_ipc_mq ipc = (pthread_ipc_mq)ptipc;

    char mq_name[30];
    sprintf(mq_name, "/dynomite%d", ptctx->tid);
    THROW_STATUS(string_copy_c(&ipc->name, (const uint8_t *)mq_name));

    ptipc->owner_ptctx = ptctx;

    // create a message queue.
    struct mq_attr attr;
    attr.mq_flags   = O_NONBLOCK;
    attr.mq_maxmsg  = 32768;
    attr.mq_msgsize = 8;
    attr.mq_curmsgs = 0;

    mode_t omask;
    omask = umask(0);
    // do a unlink first, just in case
    mq_unlink(mq_name);
    ptipc->p.sd = mq_open(mq_name, O_RDWR|O_NONBLOCK|O_CREAT|O_EXCL,
                          (S_IRWXU | S_IRWXG | S_IRWXO), &attr);
    umask(omask);
    if (ptipc->p.sd == -1) {
        ASSERT_LOG(0,"failed to open mq for reading: %s, %s\n ", mq_name, strerror(errno));
    }
    /*if (mq_unlink(mq_name) == -1) {
        log_error("failed to unlink mq %s. Please manually delete it", mq_name);
    }*/

    log_notice("Opened mq %s sd %d for reading", mq_name, ptipc->p.sd);
    thread_ctx_add_in(ptipc->owner_ptctx, get_pollable(ipc));
    return DN_OK;
}

static rstatus_t
mq_ipc_open_to_read(pthread_ipc ptipc)
{
    // open the mq for reading
    return DN_OK;
}

static rstatus_t
mq_ipc_open_to_write(pthread_ipc ptipc)
{
    // open the mq for writing
    return DN_OK;
}

static struct msg *
mq_ipc_receive(pthread_ipc ptipc)
{
    pthread_ipc_mq ipc = (pthread_ipc_mq)ptipc;
    // Make sure the owner is the guy reading from this ipc
    ASSERT(g_ptctx == ptipc->owner_ptctx);

    // read from the mq in non blocking way
    struct msg *msg = NULL;
    // we have to receive the pointer itself, hence &msg, and sizeof(msg)
    while (mq_receive(ptipc->p.sd, (char *)&msg, sizeof(msg), NULL) == -1) {
        if (errno == EAGAIN)
            return NULL;
        if (errno != EINTR) {
            log_error("failed to receive from mq: %.*s, err:%d %s", ipc->name.len,
                    ipc->name.data, errno, strerror(errno));
            return NULL;
        }
    }

    return msg;
}

static rstatus_t
mq_ipc_send(pthread_ipc ptipc, struct msg *msg)
{
    pthread_ipc_mq ipc = (pthread_ipc_mq)ptipc;
    // we have to pass the pointer itself, hence &msg, and sizeof(msg)
    if (mq_send(ptipc->p.sd, (char *)&msg, sizeof(msg), 1) != 0) {
        log_error("failed to send to mq: %.*s, err:%d %s\n", ipc->name.len,
                ipc->name.data, errno, strerror(errno));
        return DN_ERROR;
    }
    return DN_OK;
}

static rstatus_t
mq_ipc_destroy(pthread_ipc ptipc)
{
    rstatus_t status = DN_OK;
    pthread_ipc_mq ipc = (pthread_ipc_mq)ptipc;
    if (mq_close(ptipc->p.sd) == -1) {
        log_error("failed to close read end of mq: %.*s, err:%d %s\n", ipc->name.len,
                   ipc->name.data, errno, strerror(errno));
        status = DN_ERROR;
    }
    dn_free(ptipc);
    return status;
}

struct thread_ipc mq_ops = {
    {
        -1, // sd
        CONN_THREAD_IPC_MQ,
        0,// recv active
        0,// send_active
    },
    mq_ipc_init,
    mq_ipc_open_to_read,
    mq_ipc_open_to_write,
    mq_ipc_receive,
    mq_ipc_send,
    mq_ipc_destroy,
    NULL,
};

pthread_ipc
thread_ipc_mq_create(void)
{
    pthread_ipc_mq ipc = dn_zalloc(sizeof(struct thread_ipc_mq));
    ipc->ops = mq_ops;
    string_init(&ipc->name);
    return get_thread_ipc(ipc);
}
