/*
 * dyn_tls.h
 *
 * Copyright (C) 2014 Netflix, Inc.
 */

#ifndef SRC_DYN_TLS_H_
#define SRC_DYN_TLS_H_

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <memory.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

#include <openssl/rsa.h>
#include <openssl/crypto.h>
#include <openssl/x509.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/err.h>

#define CERTF "conf/dynomite.crt"
#define KEYF  "conf/dynomite.key"

#define TLS_ERR_CTX              2
#define TLS_ERR_SET_CIPHER       3
#define TLS_ERR_SET_COMPRESSION  4
#define TLS_ERR_LOAD_CERT        5
#define TLS_ERR_USE_CERT         6
#define TLS_ERR_USE_KEY          7
#define TLS_ERR_CHECK_KEY        8

struct tls_ctx {
    const SSL_METHOD *meth;
    SSL_CTX* ssl_ctx;
};

rstatus_t tls_init(struct tls_ctx *tls_ctx);
void tls_deinit(struct conn *c);
void tls_close(struct conn *c);
rstatus_t tls_accept(struct conn *s, struct conn *c, int sd);
ssize_t dyn_ssl_read(SSL *ssl,void *buf,int num);
ssize_t dyn_ssl_write(SSL *ssl,const void *buf,int num);

#endif /* SRC_DYN_TLS_H_ */
