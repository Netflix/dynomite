/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */

#ifndef DYN_CRYPTO_H_
#define DYN_CRYPTO_H_


#include <stdio.h>
#include <string.h>
#include <math.h>

#include <openssl/bio.h>
#include <openssl/evp.h>

#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/aes.h>
#include <openssl/err.h>
#include <openssl/rand.h>


#include "dyn_core.h"


#define AES_KEYLEN 32


rstatus_t crypto_init(struct context *ctx);
rstatus_t crypto_init_for_test(void);
rstatus_t crypto_deinit(void);

char* base64_encode(const unsigned char *message, const size_t length);
int base64_decode(const char *b64message, const size_t length, unsigned char **buffer);
int calc_decode_length(const char *b64input, const size_t length);

rstatus_t aes_encrypt(const unsigned char *msg, size_t msgLen, unsigned char **encMsg, unsigned char *aes_key);
rstatus_t aes_decrypt(unsigned char *encMsg, size_t encMsgLen, unsigned char **decMsg, unsigned char *aes_key);

rstatus_t dyn_aes_encrypt(const unsigned char *msg, size_t msgLen,
		                  struct mbuf *mbuf, unsigned char *aes_key);

rstatus_t dyn_aes_decrypt(unsigned char *encMsg, size_t encMsgLen,
		                  struct mbuf *mbuf, unsigned char *aes_key);

unsigned char* generate_aes_key(void);

int dyn_rsa_size(void);

rstatus_t dyn_rsa_encrypt(unsigned char *plain_msg, unsigned char *encrypted_buf);

rstatus_t dyn_rsa_decrypt(unsigned char *encrypted_msg, unsigned char *decrypted_buf);


#endif /* DYN_CRYPTO_H_ */
