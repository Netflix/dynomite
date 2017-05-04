#pragma once
#include <stdint.h>
#include <stdlib.h>
#include <printf.h>
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
struct datacenter;
struct rack;
struct dyn_ring;

extern void cleanup_charptr(char **ptr);

#define SCOPED_CHARPTR(var) \
    char * var __attribute__ ((__cleanup__(cleanup_charptr))) 

typedef enum {
    OBJ_REQ,
    OBJ_RSP,
    OBJ_CONN,
    OBJ_CONN_POOL,
    OBJ_POOL,
    OBJ_LAST
}object_type_t;

struct object;
typedef int (*func_print_t)(FILE *stream, const struct object *obj);
typedef struct object {
    uint16_t    magic;
    object_type_t type;
    func_print_t func_print;
}object_t;

void init_object(object_t *obj, object_type_t type, func_print_t func_print);

int print_obj(FILE *stream, const struct printf_info *info, const void *const *args);
int print_obj_arginfo(const struct printf_info *info, size_t n, int *argtypes);
