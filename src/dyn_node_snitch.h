
#include "dyn_core.h"


#ifndef _DYN_SNITCH_H_
#define _DYN_SNITCH_H_


unsigned char *get_broadcast_address(struct server_pool *sp);
char *get_public_hostname(struct server_pool *sp);
char *get_public_ip4(struct server_pool *sp);
char *get_private_ip4(struct server_pool *sp);
unsigned char *hostname_to_private_ip4(char *hostname);

#endif /* _DYN_SNITCH_H_s */
