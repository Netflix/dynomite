#ifdef __APPLE__
// http://clc-wiki.net/wiki/C_standard_library:string.h:NULL
// Portable C90 version: NULL ((void*)0)
# ifndef NULL
#  define NULL 0
# endif
#else
# include <libio.h> // For NULL
#endif
#include "dyn_types.h"
#include "dyn_dict_msg_id.h"

static unsigned int
dict_msg_id_hash(const void *key)
{
    msgid_t id = *(msgid_t*)key;
    return dictGenHashFunction(key, sizeof(id));
}

static int
dict_msg_id_cmp(void *privdata, const void *key1, const void *key2)
{
    msgid_t id1 = *(msgid_t*)key1;
    msgid_t id2 = *(msgid_t*)key2;
    return id1 == id2;
}

dictType msg_table_dict_type = {
    dict_msg_id_hash,            /* hash function */
    NULL,                        /* key dup */
    NULL,                        /* val dup */
    dict_msg_id_cmp,             /* key compare */
    NULL,                        /* key destructor */
    NULL                         /* val destructor */
};


