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


#define RSA_KEYLEN 2048
#define AES_KEYLEN 256
#define AES_ROUNDS 6


#define KEY_SERVER_PRI 0
#define KEY_SERVER_PUB 1
#define KEY_CLIENT_PUB 2
#define KEY_AES        3
#define KEY_AES_IV     4



//TODOs: will make this location configurable
#define PUB_KEY_FILE  "conf/dynomite_pub.key"
#define PRI_KEY_FILE  "conf/dynomite_pri.key"

rstatus_t crypto_init(void);
rstatus_t crypto_deinit(void);

char* base64_encode(const unsigned char *message, const size_t length);
int base64_decode(const char *b64message, const size_t length, unsigned char **buffer);
int calc_decode_length(const char *b64input, const size_t length);

rstatus_t aes_encrypt(const unsigned char *msg, size_t msgLen, unsigned char **encMsg);
rstatus_t aes_decrypt(unsigned char *encMsg, size_t encMsgLen, unsigned char **decMsg);

rstatus_t dyn_aes_encrypt(const unsigned char *msg, size_t msgLen,
		                  struct mbuf *mbuf, unsigned char *aes_key);

rstatus_t dyn_aes_decrypt(unsigned char *encMsg, size_t encMsgLen,
		                  struct mbuf *mbuf, unsigned char *aes_key);

unsigned char* generate_aes_key(void);

int rsa_pub_encrypt(unsigned char *data, int data_len,
		          unsigned char *key, unsigned char *encrypted);

int rsa_pub_decrypt(unsigned char *enc_data, int data_len,
		           unsigned char *key, unsigned char *decrypted);

int rsa_priv_decrypt(unsigned char *enc_data, int data_len,
		            unsigned char *key, unsigned char *decrypted);


int rsa_priv_encrypt(unsigned char *data, int data_len,
		            unsigned char *key, unsigned char *encrypted);

int crypto_test(void);

#endif /* DYN_CRYPTO_H_ */
