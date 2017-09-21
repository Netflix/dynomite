#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include<sys/socket.h>
#include<errno.h>
#include<netdb.h>
#include<arpa/inet.h>
#include <ctype.h>

#include "dyn_node_snitch.h"
#include "dyn_core.h"
#include "dyn_conf.h"
#include "dyn_string.h"
#include "dyn_util.h"



static char *broadcast_address = NULL;
static char *public_hostname = NULL;
static char *public_ip4 = NULL;
static char *private_ip4 = NULL;

static bool is_aws_env(struct server_pool *sp)
{
	return dn_strncmp(&sp->env.data, CONF_DEFAULT_ENV, 3);
}

static char *hostname_to_ip(char * hostname)
{
    struct hostent *he;
    struct in_addr **addr_list;
    int i;

    if ((he = gethostbyname(hostname)) == NULL)
    {
        return NULL;
    }

    addr_list = (struct in_addr **) he->h_addr_list;
    for(i = 0; addr_list[i] != NULL; i++);

    char *ip = dn_alloc(i);

    for(i = 0; addr_list[i] != NULL; i++)
    {
        //Return the first one;
        strcpy(ip , inet_ntoa(*(0 + addr_list[i])) );
        return ip;
    }

    return NULL;
}


char *get_broadcast_address(struct server_pool *sp)
{
	if (broadcast_address != NULL)
       return broadcast_address;

    if (is_aws_env(sp)) {
	   broadcast_address = getenv("EC2_PUBLIC_HOSTNAME");
	   if (broadcast_address != NULL)
	      return broadcast_address;
    } else {
    	broadcast_address = getenv("PUBLIC_HOSTNAME");
        if (broadcast_address != NULL)
           return broadcast_address;
    }

	struct node *peer = *(struct node **) array_get(&sp->peers, 0);
	broadcast_address = (char *) peer->name.data;
	return broadcast_address;
}

char *get_public_hostname(struct server_pool *sp)
{
	if (public_hostname != NULL)
	    return public_hostname;

	if (is_aws_env(sp)) {
        public_hostname = getenv("EC2_PUBLIC_HOSTNAME");
        if (public_hostname != NULL)
            return public_hostname;
	} else {
		public_hostname = getenv("PUBLIC_HOSTNAME");
		if (public_hostname != NULL)
    	   return public_hostname;
	}

    struct node *peer = *(struct node **) array_get(&sp->peers, 0);
    char c = (char) peer->name.data[0];
    if ((peer != NULL) && (peer->name.data != NULL) && !isdigit(c) ) {
    	public_hostname = (char *) peer->name.data;
    	return public_hostname;
    }
    return NULL;
}


char *get_public_ip4(struct server_pool *sp)
{
	if (public_ip4 != NULL)
	    return public_ip4;

	if (is_aws_env(sp)) {
       public_ip4 = getenv("EC2_PUBLIC_IPV4");
       if (public_ip4 != NULL)
    	 return public_ip4;
	} else {
		public_ip4 = getenv("PUBLIC_IPV4");
		if (public_ip4 != NULL)
		   return public_ip4;
	}

    struct node *peer = *(struct node **) array_get(&sp->peers, 0);
    if ((peer != NULL) && (peer->name.data != NULL)) {
        char c = (char) peer->name.data[0];
        if (isdigit(c))
            return (char *) peer->name.data;
    }
    return NULL;
}


char *get_private_ip4(struct server_pool *sp)
{
	if (private_ip4 != NULL)
	    return private_ip4;

	if (is_aws_env(sp)) {
       private_ip4 = getenv("EC2_LOCAL_IPV4");
       if (private_ip4 != NULL)
    	 return private_ip4;
	} else {
	   private_ip4 = getenv("LOCAL_IPV4");
	   if (private_ip4 != NULL)
          return private_ip4;
	}
    return NULL;
}

char *hostname_to_private_ip4(char *hostname)
{
   return  hostname_to_ip(hostname);
}
