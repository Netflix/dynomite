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
static RSA *rsa;

static int aes_key_size = AES_KEYLEN;
static unsigned char *aes_key;

static EVP_CIPHER_CTX *aes_encrypt_ctx;
static EVP_CIPHER_CTX *aes_decrypt_ctx;


static rstatus_t
load_private_rsa_key(void)
{
	FILE * fp;

	if(NULL != (fp= fopen(PRI_KEY_FILE, "r")) )
	{
		rsa = PEM_read_RSAPrivateKey(fp, NULL, NULL, NULL);
		if(rsa == NULL)
		{
			log_error("Could NOT read RSA private key file");
			return DN_ERROR;
		}

	} else {
		log_error("Could NOT locate RSA private key file");
		return DN_ERROR;

	}

	log_debug(LOG_INFO, "Private RSA structure filled");
	/*
	char   *pri_key;           // Private key
	char   *pub_key;           // Public key
	size_t pri_len;            // Length of private key
	size_t pub_len;            // Length of public key

	// To get the C-string PEM form:
	BIO *pri = BIO_new(BIO_s_mem());
	BIO *pub = BIO_new(BIO_s_mem());

	PEM_write_bio_RSAPrivateKey(pri, rsa, NULL, NULL, 0, NULL, NULL);
	PEM_write_bio_RSAPublicKey(pub, rsa);

	pri_len = BIO_pending(pri);
	pub_len = BIO_pending(pub);

	pri_key = malloc(pri_len + 1);
	pub_key = malloc(pub_len + 1);

	BIO_read(pri, pri_key, pri_len);
	BIO_read(pub, pub_key, pub_len);

	pri_key[pri_len] = '\0';
	pub_key[pub_len] = '\0';

	//log_debug(LOG_VERB, ("pri_key %s", pri_key);
	//log_debug(LOG_VERB, "pub_key %s", pub_key);

	BIO_free_all(pub);
	BIO_free_all(pri);

    */

	return DN_OK;
}



static rstatus_t
aes_init(void)
{
	// Init AES
	aes_cipher =  EVP_aes_128_cbc();
	aes_key = (unsigned char*) malloc(aes_key_size);


	if(RAND_bytes(aes_key, aes_key_size) == 0) {
		return DN_ERROR;
	}

	return DN_OK;
}

rstatus_t
crypto_init(void)
{
	//TODOs: check returned statuses
	// Initalize contexts
	aes_encrypt_ctx = (EVP_CIPHER_CTX*) malloc(sizeof(EVP_CIPHER_CTX));
	aes_decrypt_ctx = (EVP_CIPHER_CTX*) malloc(sizeof(EVP_CIPHER_CTX));

	EVP_CIPHER_CTX_init(aes_encrypt_ctx);

	//EVP_CIPHER_CTX_set_padding(aes_encrypt_ctx, RSA_PKCS1_PADDING);
	EVP_CIPHER_CTX_set_padding(aes_encrypt_ctx, RSA_NO_PADDING);

	EVP_CIPHER_CTX_init(aes_decrypt_ctx);

	//EVP_CIPHER_CTX_set_padding(aes_decrypt_ctx, RSA_PKCS1_PADDING);
	EVP_CIPHER_CTX_set_padding(aes_decrypt_ctx, RSA_NO_PADDING);

	//init AES
	aes_init();

	//init RSA
	load_private_rsa_key();

	return DN_OK;
}

rstatus_t
crypto_deinit(void)
{
	EVP_CIPHER_CTX_cleanup(aes_encrypt_ctx);


	EVP_CIPHER_CTX_cleanup(aes_decrypt_ctx);


	free(aes_encrypt_ctx);
	free(aes_decrypt_ctx);

	free(aes_key);

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
aes_encrypt(const unsigned char *msg, size_t msg_len, unsigned char **enc_msg, unsigned char *aes_key) {
	size_t block_len  = 0;
	size_t enc_msg_len = 0;

	*enc_msg = (unsigned char*)malloc(msg_len + AES_BLOCK_SIZE);
	if(enc_msg == NULL)
		return DN_ERROR;

	//if(!EVP_EncryptInit_ex(aes_encrypt_ctx, aes_cipher, NULL, aes_key, aes_iv)) {
	if(!EVP_EncryptInit_ex(aes_encrypt_ctx, aes_cipher, NULL, aes_key, aes_key)) {
		return DN_ERROR;
	}

	if(!EVP_EncryptUpdate(aes_encrypt_ctx, *enc_msg, (int*)&block_len, (unsigned char*)msg, msg_len)) {
		return DN_ERROR;
	}
	enc_msg_len += block_len;

	if(!EVP_EncryptFinal_ex(aes_encrypt_ctx, *enc_msg + enc_msg_len, (int*) &block_len)) {
		return DN_ERROR;
	}

	//EVP_CIPHER_CTX_cleanup(aesEncryptCtx);

	return enc_msg_len + block_len;
}


rstatus_t dyn_aes_encrypt(const unsigned char *msg, size_t msg_len, struct mbuf *mbuf, unsigned char *aes_key) {
	size_t block_len  = 0;
	size_t enc_msg_len = 0;

	ASSERT(mbuf != NULL && mbuf->last == mbuf->pos);

	//if(!EVP_EncryptInit_ex(aes_encrypt_ctx, aes_cipher, NULL, aes_key, aes_iv)) {
	if(!EVP_EncryptInit_ex(aes_encrypt_ctx, aes_cipher, NULL, aes_key, aes_key)) {
		return DN_ERROR;
	}

	if(!EVP_EncryptUpdate(aes_encrypt_ctx, mbuf->start, (int*)&block_len, (unsigned char*) msg, msg_len)) {
		return DN_ERROR;
	}
	enc_msg_len += block_len;

	if(!EVP_EncryptFinal_ex(aes_encrypt_ctx, mbuf->start + enc_msg_len, (int*) &block_len)) {
		return DN_ERROR;
	}

	EVP_CIPHER_CTX_cleanup(aes_encrypt_ctx);
	mbuf->last = mbuf->pos + enc_msg_len + block_len;

	return enc_msg_len + block_len;
}


rstatus_t dyn_aes_decrypt(unsigned char *enc_msg, size_t enc_msg_len, struct mbuf *mbuf, unsigned char *aes_key) {
	size_t dec_len   = 0;
	size_t block_len = 0;

	ASSERT(mbuf != NULL && mbuf->last == mbuf->pos);

	//if(!EVP_DecryptInit_ex(aes_decrypt_ctx, aes_cipher, NULL, aes_key, aes_iv)) {
	if(!EVP_DecryptInit_ex(aes_decrypt_ctx, aes_cipher, NULL, aes_key, aes_key)) {
		return DN_ERROR;
	}

	if(!EVP_DecryptUpdate(aes_decrypt_ctx, mbuf->pos, (int*) &block_len, enc_msg, (int)enc_msg_len)) {
		return DN_ERROR;
	}
	dec_len += block_len;

	if(!EVP_DecryptFinal_ex(aes_decrypt_ctx, mbuf->pos + dec_len, (int*) &block_len)) {
		return DN_ERROR;
	}
	dec_len += block_len;
	mbuf->last = mbuf->pos + dec_len;

	EVP_CIPHER_CTX_cleanup(aes_decrypt_ctx);

	return (int) dec_len;
}

rstatus_t
aes_decrypt(unsigned char *enc_msg, size_t enc_msg_len, unsigned char **dec_msg, unsigned char *aes_key) {
	size_t dec_len   = 0;
	size_t block_len = 0;

	*dec_msg = (unsigned char*) malloc(enc_msg_len);
	if(*dec_msg == NULL)
		return DN_ERROR;

	//if(!EVP_DecryptInit_ex(aes_decrypt_ctx, aes_cipher, NULL, aes_key, aes_iv)) {
	if(!EVP_DecryptInit_ex(aes_decrypt_ctx, aes_cipher, NULL, aes_key, aes_key)) {
		return DN_ERROR;
	}

	if(!EVP_DecryptUpdate(aes_decrypt_ctx, (unsigned char*) *dec_msg, (int*) &block_len, enc_msg, (int) enc_msg_len)) {
		return DN_ERROR;
	}
	dec_len += block_len;

	if(!EVP_DecryptFinal_ex(aes_decrypt_ctx, (unsigned char*) *dec_msg + dec_len, (int*) &block_len)) {
		return DN_ERROR;
	}
	dec_len += block_len;

	//EVP_CIPHER_CTX_cleanup(aesDecryptCtx);

	return (int)dec_len;
}


unsigned char* generate_aes_key(void) {

	//if(RAND_bytes(aes_key, AES_KEYLEN/8) == 0) {
	if(RAND_bytes(aes_key, aes_key_size) == 0) {
		return NULL;
	}

	return aes_key;
}



static int aes_test()
{
	log_debug(LOG_VERB, "aesKey is %s\n", base64_encode(aes_key, strlen(aes_key)));

	unsigned char *msg = "Hi my name is Dynomite";
	unsigned char *enc_msg = NULL;
	char *dec_msg          = NULL;
	int enc_msg_len;
	int dec_msg_len;

	log_debug(LOG_VERB, "Message to AES encrypt: %s \n", msg);

	// Encrypt the message with AES
	if((enc_msg_len = aes_encrypt((const unsigned char*)msg, strlen(msg)+1, &enc_msg, aes_key)) == -1) {
		log_debug(LOG_VERB, "AES encryption failed\n");
		return -1;
	}

	// Print the encrypted message as a base64 string
	char *b64_string = base64_encode(enc_msg, enc_msg_len);
	log_debug(LOG_VERB, "AES Encrypted message: %s\n", b64_string);

	// Decrypt the message
	if((dec_msg_len = aes_decrypt(enc_msg, (size_t)enc_msg_len, (unsigned char**) &dec_msg, aes_key)) == -1) {
		log_debug(LOG_VERB, "AES decryption failed\n");
		return -1;
	}

	log_debug(LOG_VERB, "%d bytes decrypted\n", dec_msg_len);
	log_debug(LOG_VERB, "AES Decrypted message: %s\n", dec_msg);

	free(enc_msg);
	free(dec_msg);
	free(b64_string);

	return 0;
}



rstatus_t
dyn_rsa_encrypt(unsigned char *plain_msg, unsigned char *encrypted_buf)
{
	if(RSA_public_encrypt(32, plain_msg, encrypted_buf, rsa, RSA_PKCS1_OAEP_PADDING) != 128) {
		ERR_load_crypto_strings();
		char  err[130];
		ERR_error_string(ERR_get_error(), err);
		log_debug(LOG_VERB, "Error in encrypting message: %s\n", err);
		return DN_ERROR;
	}
	return 128;
}

rstatus_t
dyn_rsa_decrypt(unsigned char *encrypted_msg, unsigned char *decrypted_buf)
{
	if(RSA_private_decrypt(128,
			               encrypted_msg,
					       decrypted_buf,
					       rsa, RSA_PKCS1_OAEP_PADDING) != 32) {
				ERR_load_crypto_strings();
				char  err[130];
				ERR_error_string(ERR_get_error(), err);
				log_debug(LOG_VERB, "Error in decrypting message: %s\n", err);
				return DN_ERROR;
	}

	return 32;
}


static int rsa_test()
{
	static unsigned char encrypted_buf[130];
	static unsigned char decrypted_buf[34];
	static unsigned char *msg;

	int i=0;
	for(; i<10; i++) {
		msg = generate_aes_key();

		log_debug(LOG_VERB, "i = %d", i);
		log_debug(LOG_VERB, "AES key           : %s \n", base64_encode(msg, 32));


		dyn_rsa_encrypt(msg, encrypted_buf);

		dyn_rsa_decrypt(encrypted_buf, decrypted_buf);

		log_debug(LOG_VERB, "Decrypted message : %s \n", base64_encode(decrypted_buf, 32));
	}

	return 0;
}

void crypto_test(void)
{
	aes_test();
	rsa_test();
}
