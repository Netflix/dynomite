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

static unsigned char *aesKey;
static unsigned char *aesIV;


static EVP_CIPHER_CTX *rsaEncryptCtx;
static EVP_CIPHER_CTX *aesEncryptCtx;

static EVP_CIPHER_CTX *rsaDecryptCtx;
static EVP_CIPHER_CTX *aesDecryptCtx;


static rstatus_t
loadPrivateRSAKey()
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
loadPublicRSAKey()
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
init_aes()
{
	// Init AES
	aes_cipher =  EVP_aes_256_cbc();
	aesKey = (unsigned char*) malloc(AES_KEYLEN/8);
	aesIV = (unsigned char*) malloc(AES_KEYLEN/8);

    if(RAND_bytes(aesKey, AES_KEYLEN/8) == 0) {
         return DN_ERROR;
    }

    if(RAND_bytes(aesIV, AES_KEYLEN/8) == 0) {
         return DN_ERROR;
    }

    return DN_OK;
}

rstatus_t
init_crypto()
{
	//TODOs: check returned statuses

	//init context
	// Initalize contexts
	rsaEncryptCtx = (EVP_CIPHER_CTX*) malloc(sizeof(EVP_CIPHER_CTX));
	aesEncryptCtx = (EVP_CIPHER_CTX*) malloc(sizeof(EVP_CIPHER_CTX));

	rsaDecryptCtx = (EVP_CIPHER_CTX*) malloc(sizeof(EVP_CIPHER_CTX));
	aesDecryptCtx = (EVP_CIPHER_CTX*) malloc(sizeof(EVP_CIPHER_CTX));

	// Always a good idea to check if malloc failed
	if(rsaEncryptCtx == NULL || aesEncryptCtx == NULL ||
	   rsaDecryptCtx == NULL || aesDecryptCtx == NULL) {
		return DN_ERROR;
	}

	EVP_CIPHER_CTX_init(rsaEncryptCtx);
	EVP_CIPHER_CTX_set_padding(rsaEncryptCtx, RSA_PKCS1_PADDING);

    EVP_CIPHER_CTX_init(rsaDecryptCtx);
    EVP_CIPHER_CTX_set_padding(rsaDecryptCtx, RSA_PKCS1_PADDING);

    EVP_CIPHER_CTX_init(aesEncryptCtx);
    EVP_CIPHER_CTX_set_padding(aesEncryptCtx, RSA_PKCS1_PADDING);

    EVP_CIPHER_CTX_init(aesDecryptCtx);
    EVP_CIPHER_CTX_set_padding(aesDecryptCtx, RSA_PKCS1_PADDING);

	//init RSA
	loadPublicRSAKey();
	loadPrivateRSAKey();

	//init AES
	init_aes();

	return DN_OK;
}

rstatus_t
deinit_crypto()
{
	EVP_CIPHER_CTX_cleanup(rsaEncryptCtx);
	EVP_CIPHER_CTX_cleanup(aesEncryptCtx);

	EVP_CIPHER_CTX_cleanup(rsaDecryptCtx);
	EVP_CIPHER_CTX_cleanup(aesDecryptCtx);

	free(rsaEncryptCtx);
	free(aesEncryptCtx);

	free(rsaDecryptCtx);
	free(aesDecryptCtx);

	free(aesKey);
	free(aesIV);

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


char*
base64Encode(const unsigned char *message, const size_t length) {
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
base64Decode(const char *b64message, const size_t length, unsigned char **buffer) {
    BIO *bio;
    BIO *b64;
    int decodedLength = calcDecodeLength(b64message, length);

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
calcDecodeLength(const char *b64input, const size_t length) {
    int padding = 0;

    // Check for trailing '=''s as padding
    if(b64input[length-1] == '=' && b64input[length-2] == '=')
        padding = 2;
    else if (b64input[length-1] == '=')
        padding = 1;

    return (int)length*0.75 - padding;
}

rstatus_t
aesEncrypt(const unsigned char *msg, size_t msgLen, unsigned char **encMsg) {
    size_t blockLen  = 0;
    size_t encMsgLen = 0;

    *encMsg = (unsigned char*)malloc(msgLen + AES_BLOCK_SIZE);
    if(encMsg == NULL)
    	return DN_ERROR;

    if(!EVP_EncryptInit_ex(aesEncryptCtx, aes_cipher, NULL, aesKey, aesIV)) {
        return DN_ERROR;
    }

    if(!EVP_EncryptUpdate(aesEncryptCtx, *encMsg, (int*)&blockLen, (unsigned char*)msg, msgLen)) {
        return DN_ERROR;
    }
    encMsgLen += blockLen;

    if(!EVP_EncryptFinal_ex(aesEncryptCtx, *encMsg + encMsgLen, (int*)&blockLen)) {
        return DN_ERROR;
    }

    //EVP_CIPHER_CTX_cleanup(aesEncryptCtx);

    return encMsgLen + blockLen;
}


rstatus_t
aesDecrypt(unsigned char *encMsg, size_t encMsgLen, unsigned char **decMsg) {
    size_t decLen   = 0;
    size_t blockLen = 0;

    *decMsg = (unsigned char*)malloc(encMsgLen);
    if(*decMsg == NULL)
    	return DN_ERROR;

    if(!EVP_DecryptInit_ex(aesDecryptCtx, aes_cipher, NULL, aesKey, aesIV)) {
        return DN_ERROR;
    }

    if(!EVP_DecryptUpdate(aesDecryptCtx, (unsigned char*)*decMsg, (int*)&blockLen, encMsg, (int)encMsgLen)) {
        return DN_ERROR;
    }
    decLen += blockLen;

    if(!EVP_DecryptFinal_ex(aesDecryptCtx, (unsigned char*)*decMsg + decLen, (int*)&blockLen)) {
        return DN_ERROR;
    }
    decLen += blockLen;

    //EVP_CIPHER_CTX_cleanup(aesDecryptCtx);

    return (int)decLen;
}


int test_crypto()
{
	loga("aesKey is %s\n", base64Encode(aesKey, strlen(aesKey)));
	loga("aesIV is %s\n", base64Encode(aesIV, strlen(aesIV)));

	unsigned char *msg = "Hi my name is Justin";
	unsigned char *encMsg = NULL;
	char *decMsg          = NULL;
	int encMsgLen;
	int decMsgLen;

	loga("Message to AES encrypt: %s \n", msg);

	// Encrypt the message with AES
	if((encMsgLen = aesEncrypt((const unsigned char*)msg, strlen(msg)+1, &encMsg)) == -1) {
		fprintf(stderr, "Encryption failed\n");
		return 1;
	}

	// Print the encrypted message as a base64 string
	char *b64String = base64Encode(encMsg, encMsgLen);
	printf("AES Encrypted message: %s\n", b64String);

	// Decrypt the message
	if((decMsgLen = aesDecrypt(encMsg, (size_t)encMsgLen, (unsigned char**)&decMsg)) == -1) {
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
