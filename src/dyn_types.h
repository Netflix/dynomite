#pragma once
#include <stdint.h>
typedef uint64_t msgid_t;
typedef uint64_t msec_t;
typedef uint64_t usec_t;
typedef uint64_t sec_t;

typedef enum {
    SECURE_OPTION_NONE,
    SECURE_OPTION_RACK,
    SECURE_OPTION_DC,
    SECURE_OPTION_ALL,
}secure_server_option_t;

struct array;
struct string;
struct context;
struct conn;
struct conn_tqh;
struct msg;
struct msg_tqh;
struct server;
struct server_pool;
struct mbuf;
struct mhdr;
struct conf;
struct stats;
struct entropy_conn;
struct instance;
struct event_base;
struct rack;
struct dyn_ring;
