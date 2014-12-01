/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */

#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/rsa.h>
#include <openssl/evp.h>
#include <openssl/bio.h>
#include <openssl/err.h>
#include <stdio.h>

#include "dyn_crypto.h"

static EVP_CIPHER *aes_cipher;
static RSA *public_rsa;
static RSA *private_rsa;

static unsigned char *aes_key;
static unsigned char *aes_iv;


static EVP_CIPHER_CTX *rsa_encrypt_ctx;
static EVP_CIPHER_CTX *aes_encrypt_ctx;

static EVP_CIPHER_CTX *rsa_decrypt_ctx;
static EVP_CIPHER_CTX *aes_decrypt_ctx;


static rstatus_t
load_private_rsa_key(void)
{
	FILE * fp;

	if(NULL != (fp= fopen(PRI_KEY_FILE, "r")) )
	{
		private_rsa = PEM_read_RSAPrivateKey(fp, NULL, NULL, NULL);
		if(private_rsa == NULL)
		{
			log_error("Could NOT read RSA private key file");
			return DN_ERROR;
		}

	} else {
		log_error("Could NOT locate RSA private key file");
		return DN_ERROR;

	}

	loga("Private RSA structure filled");
	return DN_OK;
}


static rstatus_t
load_public_rsa_key(void)
{
	FILE * fp;

	if(NULL != (fp= fopen(PUB_KEY_FILE, "r")) )
	{
		public_rsa = PEM_read_RSA_PUBKEY(fp, NULL, NULL, NULL);
		if (public_rsa == NULL)
		{
			log_error("Could NOT read RSA public key file");
			return DN_ERROR;
		}

	} else {
		log_error("Could NOT locate RSA public key file");
		return DN_ERROR;

	}

	loga("Public RSA structure filled");
	return DN_OK;
}

static rstatus_t
aes_init(void)
{
	// Init AES
	aes_cipher =  EVP_aes_256_cbc();
	aes_key = (unsigned char*) malloc(AES_KEYLEN/8);
	aes_iv = (unsigned char*) malloc(AES_KEYLEN/8);

	if(RAND_bytes(aes_key, AES_KEYLEN/8) == 0) {
		return DN_ERROR;
	}

	if(RAND_bytes(aes_iv, AES_KEYLEN/8) == 0) {
		return DN_ERROR;
	}

	return DN_OK;
}

rstatus_t
crypto_init(void)
{
	//TODOs: check returned statuses

	// Initalize contexts
	rsa_encrypt_ctx = (EVP_CIPHER_CTX*) malloc(sizeof(EVP_CIPHER_CTX));
	aes_encrypt_ctx = (EVP_CIPHER_CTX*) malloc(sizeof(EVP_CIPHER_CTX));

	rsa_decrypt_ctx = (EVP_CIPHER_CTX*) malloc(sizeof(EVP_CIPHER_CTX));
	aes_decrypt_ctx = (EVP_CIPHER_CTX*) malloc(sizeof(EVP_CIPHER_CTX));

	// Always a good idea to check if malloc failed
	if(rsa_encrypt_ctx == NULL || aes_encrypt_ctx == NULL ||
			rsa_decrypt_ctx == NULL || aes_decrypt_ctx == NULL) {
		return DN_ERROR;
	}

	EVP_CIPHER_CTX_init(rsa_encrypt_ctx);
	EVP_CIPHER_CTX_set_padding(rsa_encrypt_ctx, RSA_PKCS1_PADDING);

	EVP_CIPHER_CTX_init(rsa_decrypt_ctx);
	EVP_CIPHER_CTX_set_padding(rsa_decrypt_ctx, RSA_PKCS1_PADDING);

	EVP_CIPHER_CTX_init(aes_encrypt_ctx);
	EVP_CIPHER_CTX_set_padding(aes_encrypt_ctx, RSA_PKCS1_PADDING);

	EVP_CIPHER_CTX_init(aes_decrypt_ctx);
	EVP_CIPHER_CTX_set_padding(aes_decrypt_ctx, RSA_PKCS1_PADDING);

	//init RSA
	load_public_rsa_key();
	load_private_rsa_key();

	//init AES
	aes_init();

	return DN_OK;
}

rstatus_t
crypto_deinit(void)
{
	EVP_CIPHER_CTX_cleanup(rsa_encrypt_ctx);
	EVP_CIPHER_CTX_cleanup(aes_encrypt_ctx);

	EVP_CIPHER_CTX_cleanup(rsa_decrypt_ctx);
	EVP_CIPHER_CTX_cleanup(aes_decrypt_ctx);

	free(rsa_encrypt_ctx);
	free(aes_encrypt_ctx);

	free(rsa_decrypt_ctx);
	free(aes_decrypt_ctx);

	free(aes_key);
	free(aes_iv);

	return DN_OK;
}

char*
base64_encode(const unsigned char *message, const size_t length) {
	BIO *bio;
	BIO *b64;
	FILE* stream;

	int encodedSize = 4*ceil((double)length/3);
	char *buffer = (char*)malloc(encodedSize+1);
	if(buffer == NULL) {
		fprintf(stderr, "Failed to allocate memory\n");
		exit(1);
	}

	stream = fmemopen(buffer, encodedSize+1, "w");
	b64 = BIO_new(BIO_f_base64());
	bio = BIO_new_fp(stream, BIO_NOCLOSE);
	bio = BIO_push(b64, bio);
	BIO_set_flags(bio, BIO_FLAGS_BASE64_NO_NL);
	BIO_write(bio, message, length);
	(void)BIO_flush(bio);
	BIO_free_all(bio);
	fclose(stream);

	return buffer;
}


int
base64_decode(const char *b64message, const size_t length, unsigned char **buffer) {
	BIO *bio;
	BIO *b64;
	int decodedLength = calc_decode_length(b64message, length);

	*buffer = (unsigned char*)malloc(decodedLength+1);
	if(*buffer == NULL) {
		fprintf(stderr, "Failed to allocate memory\n");
		exit(1);
	}
	FILE* stream = fmemopen((char*)b64message, length, "r");

	b64 = BIO_new(BIO_f_base64());
	bio = BIO_new_fp(stream, BIO_NOCLOSE);
	bio = BIO_push(b64, bio);
	BIO_set_flags(bio, BIO_FLAGS_BASE64_NO_NL);
	decodedLength = BIO_read(bio, *buffer, length);
	(*buffer)[decodedLength] = '\0';

	BIO_free_all(bio);
	fclose(stream);

	return decodedLength;
}


int
calc_decode_length(const char *b64input, const size_t length) {
	int padding = 0;

	// Check for trailing '=''s as padding
	if(b64input[length-1] == '=' && b64input[length-2] == '=')
		padding = 2;
	else if (b64input[length-1] == '=')
		padding = 1;

	return (int)length*0.75 - padding;
}

rstatus_t
aes_encrypt(const unsigned char *msg, size_t msgLen, unsigned char **encMsg) {
	size_t blockLen  = 0;
	size_t encMsgLen = 0;

	*encMsg = (unsigned char*)malloc(msgLen + AES_BLOCK_SIZE);
	if(encMsg == NULL)
		return DN_ERROR;

	//if(!EVP_EncryptInit_ex(aes_encrypt_ctx, aes_cipher, NULL, aes_key, aes_iv)) {
	if(!EVP_EncryptInit_ex(aes_encrypt_ctx, aes_cipher, NULL, aes_key, aes_key)) {
		return DN_ERROR;
	}

	if(!EVP_EncryptUpdate(aes_encrypt_ctx, *encMsg, (int*)&blockLen, (unsigned char*)msg, msgLen)) {
		return DN_ERROR;
	}
	encMsgLen += blockLen;

	if(!EVP_EncryptFinal_ex(aes_encrypt_ctx, *encMsg + encMsgLen, (int*)&blockLen)) {
		return DN_ERROR;
	}

	//EVP_CIPHER_CTX_cleanup(aesEncryptCtx);

	return encMsgLen + blockLen;
}


rstatus_t
aes_decrypt(unsigned char *encMsg, size_t encMsgLen, unsigned char **decMsg) {
	size_t decLen   = 0;
	size_t blockLen = 0;

	*decMsg = (unsigned char*)malloc(encMsgLen);
	if(*decMsg == NULL)
		return DN_ERROR;

	//if(!EVP_DecryptInit_ex(aes_decrypt_ctx, aes_cipher, NULL, aes_key, aes_iv)) {
	if(!EVP_DecryptInit_ex(aes_decrypt_ctx, aes_cipher, NULL, aes_key, aes_key)) {
		return DN_ERROR;
	}

	if(!EVP_DecryptUpdate(aes_decrypt_ctx, (unsigned char*)*decMsg, (int*)&blockLen, encMsg, (int)encMsgLen)) {
		return DN_ERROR;
	}
	decLen += blockLen;

	if(!EVP_DecryptFinal_ex(aes_decrypt_ctx, (unsigned char*)*decMsg + decLen, (int*)&blockLen)) {
		return DN_ERROR;
	}
	decLen += blockLen;

	//EVP_CIPHER_CTX_cleanup(aesDecryptCtx);

	return (int)decLen;
}


unsigned char* generate_aes_key(void) {

	if(RAND_bytes(aes_key, AES_KEYLEN/8) == 0) {
		return NULL;
	}

	return aes_key;
}


int rsa_pub_encrypt(unsigned char *data, int data_len,
		unsigned char *key, unsigned char *encrypted)
{
	return RSA_public_encrypt(data_len, data, encrypted, public_rsa, RSA_PKCS1_PADDING);
}


int rsa_pub_decrypt(unsigned char *enc_data, int data_len,
		unsigned char *key, unsigned char *decrypted)
{
	return RSA_public_decrypt(data_len,enc_data, decrypted, public_rsa, RSA_PKCS1_PADDING);
}


int rsa_priv_decrypt(unsigned char *enc_data, int data_len,
		unsigned char *key, unsigned char *decrypted)
{
	return RSA_private_decrypt(data_len, enc_data, decrypted, private_rsa, RSA_PKCS1_PADDING);
}


int rsa_priv_encrypt(unsigned char *data, int data_len,
		unsigned char *key, unsigned char *encrypted)
{
	return RSA_private_encrypt(data_len,data, encrypted, private_rsa, RSA_PKCS1_PADDING);
}




int crypto_test()
{
	loga("aesKey is %s\n", base64_encode(aes_key, strlen(aes_key)));
	loga("aesIV is %s\n", base64_encode(aes_iv, strlen(aes_iv)));

	unsigned char *msg = "Hi my name is Justin";
	unsigned char *encMsg = NULL;
	char *decMsg          = NULL;
	int encMsgLen;
	int decMsgLen;

	loga("Message to AES encrypt: %s \n", msg);

	// Encrypt the message with AES
	if((encMsgLen = aes_encrypt((const unsigned char*)msg, strlen(msg)+1, &encMsg)) == -1) {
		fprintf(stderr, "Encryption failed\n");
		return 1;
	}

	// Print the encrypted message as a base64 string
	char *b64String = base64_encode(encMsg, encMsgLen);
	printf("AES Encrypted message: %s\n", b64String);

	// Decrypt the message
	if((decMsgLen = aes_decrypt(encMsg, (size_t)encMsgLen, (unsigned char**)&decMsg)) == -1) {
		fprintf(stderr, "Decryption failed\n");
		return 1;
	}
	printf("%d bytes decrypted\n", decMsgLen);
	printf("AES Decrypted message: %s\n", decMsg);

	// Memory leaks... yadda yadda yadda...
	free(encMsg);
	free(decMsg);
	free(b64String);


}
