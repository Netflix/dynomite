/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed
 * storages. Copyright (C) 2014 Netflix, Inc.
 */

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/ssl.h>
#include <stdio.h>

#include "dyn_core.h"
#include "dyn_crypto.h"
#include "dyn_server.h"

static const EVP_CIPHER *aes_cipher;
static RSA *rsa;
static int rsa_size = 0;

static unsigned char aes_key[AES_KEYLEN + 1];

static EVP_CIPHER_CTX *aes_encrypt_ctx;
static EVP_CIPHER_CTX *aes_decrypt_ctx;

/**
 * Read the PEM key file.
 * @param[in,out] pem_key_file PEM key file.
 * @return rstatus_t Return status code.
 */
static rstatus_t load_private_rsa_key_by_file(
    const struct string *pem_key_file) {
  FILE *fp;

  if (string_empty(pem_key_file)) {
    log_error("Error: PEM key file name is empty. Unable to read file.");
    return DN_ERROR;
  }

  char file_name[pem_key_file->len + 1];
  memcpy(file_name, pem_key_file->data, pem_key_file->len);
  file_name[pem_key_file->len] = '\0';

  if (access(file_name, F_OK) < 0) {
    log_error("Error: file %s does not exist", file_name);
    return DN_ERROR;
  }

  if (NULL != (fp = fopen(file_name, "r"))) {
    rsa = PEM_read_RSAPrivateKey(fp, NULL, NULL, NULL);
    if (rsa == NULL) {
      log_error("Error: could NOT read RSA pem key file at %s", file_name);
      return DN_ERROR;
    }

  } else {
    log_error("Error: could NOT locate RSA pem key file at %s", file_name);
    return DN_ERROR;
  }

  rsa_size = RSA_size(rsa);

  log_debug(LOG_INFO, "Private RSA structure filled with size %d", rsa_size);
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

/**
 * Load the RSA PEM key file.
 * @param[in] sp Server pool.
 * @return rstatus_t Return status code.
 */
static rstatus_t load_private_rsa_key(struct server_pool *sp) {
  if (sp == NULL || string_empty(&sp->pem_key_file)) {
    log_error(
        "Could NOT read RSA pem key file due to bad context or configuration");
    return DN_ERROR;
  }

  THROW_STATUS(load_private_rsa_key_by_file(&sp->pem_key_file));
  return DN_OK;
}

/**
 * Initialize AES.
 * @return rstatus_t Return status code.
 */
static rstatus_t aes_init(void) {
// Initialize contexts
#if OPENSSL_VERSION_NUMBER < 0x10100000L
  aes_encrypt_ctx = (EVP_CIPHER_CTX *)malloc(sizeof(EVP_CIPHER_CTX));
  aes_decrypt_ctx = (EVP_CIPHER_CTX *)malloc(sizeof(EVP_CIPHER_CTX));

  if (aes_encrypt_ctx == NULL || aes_decrypt_ctx == NULL) {
    return DN_ENOMEM;
  }

  EVP_CIPHER_CTX_init(aes_encrypt_ctx);
  EVP_CIPHER_CTX_init(aes_decrypt_ctx);
#else

  aes_encrypt_ctx = EVP_CIPHER_CTX_new();
  aes_decrypt_ctx = EVP_CIPHER_CTX_new();

  EVP_CIPHER_CTX_reset(aes_encrypt_ctx);
  EVP_CIPHER_CTX_reset(aes_decrypt_ctx);
#endif

  // EVP_CIPHER_CTX_set_padding(aes_encrypt_ctx, RSA_PKCS1_PADDING);
  EVP_CIPHER_CTX_set_padding(aes_encrypt_ctx, RSA_NO_PADDING);

  // EVP_CIPHER_CTX_set_padding(aes_decrypt_ctx, RSA_PKCS1_PADDING);
  EVP_CIPHER_CTX_set_padding(aes_decrypt_ctx, RSA_NO_PADDING);

  // Init AES
  aes_cipher = EVP_aes_128_cbc();

  if (RAND_bytes(aes_key, AES_KEYLEN) == 0) {
    return DN_ERROR;
  }

  return DN_OK;
}

/**
 * Initialize AES and load the PEM key file.
 * @param[in] sp Server pool.
 * @return rstatus_t Return status code.
 */
rstatus_t crypto_init(struct server_pool *sp) {
  if (sp->secure_server_option == SECURE_OPTION_NONE) {
    log_debug(LOG_NOTICE,
              "secure_server_option is none, skipping crypto_init()");
    return DN_OK;
  }

  // init AES
  THROW_STATUS(aes_init());
  // init RSA
  THROW_STATUS(load_private_rsa_key(sp));
  return DN_OK;
}

rstatus_t crypto_deinit(void) {
  if (aes_encrypt_ctx != NULL) {
#if OPENSSL_VERSION_NUMBER < 0x10100000L
    EVP_CIPHER_CTX_cleanup(aes_encrypt_ctx);
    free(aes_encrypt_ctx);
#else
    EVP_CIPHER_CTX_free(aes_encrypt_ctx);
#endif
  }

  if (aes_decrypt_ctx != NULL) {
#if OPENSSL_VERSION_NUMBER < 0x10100000L
    EVP_CIPHER_CTX_cleanup(aes_decrypt_ctx);
    free(aes_decrypt_ctx);
#else
    EVP_CIPHER_CTX_free(aes_decrypt_ctx);
#endif
  }
  return DN_OK;
}

char *base64_encode(const unsigned char *message, const size_t length) {
  BIO *bio;
  BIO *b64;
  FILE *stream;

  size_t encodedSize = (size_t)(4 * ceil((double)length / 3));
  char *buffer = (char *)malloc(encodedSize + 1);
  if (buffer == NULL) {
    fprintf(stderr, "Failed to allocate memory\n");
    exit(1);
  }

  stream = fmemopen(buffer, (size_t)encodedSize + 1, "w");
  b64 = BIO_new(BIO_f_base64());
  bio = BIO_new_fp(stream, BIO_NOCLOSE);
  bio = BIO_push(b64, bio);
  BIO_set_flags(bio, BIO_FLAGS_BASE64_NO_NL);
  BIO_write(bio, message, (int)length);
  (void)BIO_flush(bio);
  BIO_free_all(bio);
  fclose(stream);

  return buffer;
}

/*
static int
calc_decode_length(const char *b64input, const size_t length)
{
    int padding = 0;

    // Check for trailing '=''s as padding
    if(b64input[length-1] == '=' && b64input[length-2] == '=')
        padding = 2;
    else if (b64input[length-1] == '=')
        padding = 1;

    return (int)length*0.75 - padding;
}

static int
base64_decode(const char *b64message, const size_t length, unsigned char
**buffer)
{
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
*/

rstatus_t aes_encrypt(const unsigned char *msg, size_t msg_len,
                      unsigned char **enc_msg, unsigned char *arg_aes_key) {
  int block_len = 0;
  int enc_msg_len = 0;

  *enc_msg = (unsigned char *)malloc(msg_len + AES_BLOCK_SIZE);
  if (*enc_msg == NULL) return DN_ERROR;

  // if(!EVP_EncryptInit_ex(aes_encrypt_ctx, aes_cipher, NULL, arg_aes_key,
  // aes_iv)) {
  if (!EVP_EncryptInit_ex(aes_encrypt_ctx, aes_cipher, NULL, arg_aes_key,
                          arg_aes_key)) {
    log_debug(LOG_VERB, "This is bad data in EVP_EncryptInit_ex : '%.*s'",
              msg_len, msg);
    return DN_ERROR;
  }

  if (!EVP_EncryptUpdate(aes_encrypt_ctx, *enc_msg, &block_len,
                         (unsigned char *)msg, (int)msg_len)) {
    log_debug(LOG_VERB, "This is bad data in EVP_EncryptUpdate : '%.*s'",
              msg_len, msg);
    return DN_ERROR;
  }
  enc_msg_len += block_len;

  if (!EVP_EncryptFinal_ex(aes_encrypt_ctx, *enc_msg + enc_msg_len,
                           &block_len)) {
    log_debug(LOG_VERB, "This is bad data in EVP_EncryptFinal_ex : '%.*s'",
              msg_len, msg);
    return DN_ERROR;
  }

  // EVP_CIPHER_CTX_cleanup(aesEncryptCtx);

  return enc_msg_len + block_len;
}

rstatus_t dyn_aes_encrypt(const unsigned char *msg, size_t msg_len,
                          struct mbuf *mbuf, unsigned char *arg_aes_key) {
  int block_len = 0;
  int enc_msg_len = 0;

  ASSERT(mbuf != NULL && mbuf->last == mbuf->pos);

  // if(!EVP_EncryptInit_ex(aes_encrypt_ctx, aes_cipher, NULL, arg_aes_key,
  // aes_iv)) {
  if (!EVP_EncryptInit_ex(aes_encrypt_ctx, aes_cipher, NULL, arg_aes_key,
                          arg_aes_key)) {
    log_hexdump(
        LOG_DEBUG, msg, msg_len,
        "Bad data in EVP_EncryptInit_ex, crypto data with %ld bytes of data",
        msg_len);
    return DN_ERROR;
  }

  if (!EVP_EncryptUpdate(aes_encrypt_ctx, mbuf->start, &block_len,
                         (unsigned char *)msg, (int)msg_len)) {
    log_hexdump(
        LOG_DEBUG, msg, msg_len,
        "Bad data in EVP_EncryptUpdate, crypto data with %ld bytes of data",
        msg_len);
    return DN_ERROR;
  }
  enc_msg_len += block_len;

  if (!EVP_EncryptFinal_ex(aes_encrypt_ctx, mbuf->start + enc_msg_len,
                           &block_len)) {
    log_hexdump(
        LOG_DEBUG, msg, msg_len,
        "Bad data in EVP_EncryptFinal_ex, crypto data with %ld bytes of data",
        msg_len);
    return DN_ERROR;
  }

#if OPENSSL_VERSION_NUMBER < 0x10100000L
  EVP_CIPHER_CTX_cleanup(aes_encrypt_ctx);
#else
  EVP_CIPHER_CTX_reset(aes_encrypt_ctx);
#endif

  // for encrypt, we allow to use up to the extra space
  if (enc_msg_len + block_len > mbuf->end_extra - mbuf->last) {
    return DN_ERROR;
  }

  mbuf->last = mbuf->pos + enc_msg_len + block_len;

  return enc_msg_len + block_len;
}

rstatus_t dyn_aes_decrypt(unsigned char *enc_msg, size_t enc_msg_len,
                          struct mbuf *mbuf, unsigned char *arg_aes_key) {
  if (ENCRYPTION) {
    size_t dec_len = 0;
    size_t block_len = 0;

    ASSERT(mbuf != NULL && mbuf->start == mbuf->pos);

    // if(!EVP_DecryptInit_ex(aes_decrypt_ctx, aes_cipher, NULL, arg_aes_key,
    // aes_iv)) {
    if (!EVP_DecryptInit_ex(aes_decrypt_ctx, aes_cipher, NULL, arg_aes_key,
                            arg_aes_key)) {
      log_hexdump(
          LOG_DEBUG, enc_msg, enc_msg_len,
          "Bad data in EVP_DecryptInit_ex, crypto data with %ld bytes of data",
          enc_msg_len);
      return DN_ERROR;
    }

    if (!EVP_DecryptUpdate(aes_decrypt_ctx, mbuf->pos, (int *)&block_len,
                           enc_msg, (int)enc_msg_len)) {
      log_hexdump(
          LOG_DEBUG, enc_msg, enc_msg_len,
          "Bad data in EVP_DecryptUpdate, crypto data with %ld bytes of data",
          enc_msg_len);
      return DN_ERROR;
    }
    dec_len += block_len;

    if (!EVP_DecryptFinal_ex(aes_decrypt_ctx, mbuf->pos + dec_len,
                             (int *)&block_len)) {
      log_hexdump(
          LOG_DEBUG, enc_msg, enc_msg_len,
          "Bad data in EVP_DecryptFinal_ex, crypto data with %ld bytes of data",
          enc_msg_len);
      return DN_ERROR;
    }

    dec_len += block_len;
    mbuf->last = mbuf->pos + dec_len;

#if OPENSSL_VERSION_NUMBER < 0x10100000L
    EVP_CIPHER_CTX_cleanup(aes_decrypt_ctx);
#else
    EVP_CIPHER_CTX_reset(aes_decrypt_ctx);
#endif

    return (int)dec_len;
  }

  mbuf_copy(mbuf, enc_msg, enc_msg_len);
  return (int)enc_msg_len;
}

/*
 *  AES encrypt a msg with one or more buffers
 *
 */
rstatus_t dyn_aes_encrypt_msg(struct msg *msg, unsigned char *arg_aes_key,
                              size_t *outlen) {
  struct mhdr mhdr_tem;
  int count = 0;

  if (STAILQ_EMPTY(&msg->mhdr)) {
    // 'msg' is empty. Nothing to encrypt.
    return DN_ERROR;
  }

  STAILQ_INIT(&mhdr_tem);

  struct mbuf *mbuf;
  while (!STAILQ_EMPTY(&msg->mhdr)) {
    mbuf = STAILQ_FIRST(&msg->mhdr);
    // STAILQ_REMOVE_HEAD(&msg->mhdr, next);
    mbuf_remove(&msg->mhdr, mbuf);

    // mbuf_dump(mbuf);

    struct mbuf *nbuf = mbuf_get();
    if (nbuf == NULL) {
      // Unable to obtain an 'mbuf'.
      mbuf_put(mbuf);
      return DN_ENOMEM;
    }

    int n = dyn_aes_encrypt(mbuf->pos, mbuf_length(mbuf), nbuf, arg_aes_key);
    if (n > 0) count += n;

    mbuf_put(mbuf);
    // mbuf_dump(nbuf);
    if (STAILQ_EMPTY(&mhdr_tem)) {
      STAILQ_INSERT_HEAD(&mhdr_tem, nbuf, next);
    } else {
      STAILQ_INSERT_TAIL(&mhdr_tem, nbuf, next);
    }
  }

  while (!STAILQ_EMPTY(&mhdr_tem)) {
    mbuf = STAILQ_FIRST(&mhdr_tem);
    // STAILQ_REMOVE_HEAD(&mhdr_tem, next);
    mbuf_remove(&mhdr_tem, mbuf);

    if (STAILQ_EMPTY(&msg->mhdr)) {
      STAILQ_INSERT_HEAD(&msg->mhdr, mbuf, next);
    } else {
      STAILQ_INSERT_TAIL(&msg->mhdr, mbuf, next);
    }
  }

  *outlen = count;
  return DN_OK;
}

rstatus_t aes_decrypt(unsigned char *enc_msg, size_t enc_msg_len,
                      unsigned char **dec_msg, unsigned char *arg_aes_key) {
  size_t dec_len = 0;
  size_t block_len = 0;

  *dec_msg = (unsigned char *)malloc(enc_msg_len);
  if (*dec_msg == NULL) return DN_ERROR;

  // if(!EVP_DecryptInit_ex(aes_decrypt_ctx, aes_cipher, NULL, arg_aes_key,
  // aes_iv)) {
  if (!EVP_DecryptInit_ex(aes_decrypt_ctx, aes_cipher, NULL, arg_aes_key,
                          arg_aes_key)) {
    log_debug(LOG_VERB, "This is bad data in EVP_DecryptInit_ex : '%.*s'",
              enc_msg_len, enc_msg);
    return DN_ERROR;
  }

  if (!EVP_DecryptUpdate(aes_decrypt_ctx, (unsigned char *)*dec_msg,
                         (int *)&block_len, enc_msg, (int)enc_msg_len)) {
    log_debug(LOG_VERB, "This is bad data in EVP_DecryptUpdate : '%.*s'",
              enc_msg_len, enc_msg);
    return DN_ERROR;
  }
  dec_len += block_len;

  if (!EVP_DecryptFinal_ex(aes_decrypt_ctx, (unsigned char *)*dec_msg + dec_len,
                           (int *)&block_len)) {
    log_debug(LOG_VERB, "This is bad data in EVP_DecryptFinal_ex : '%.*s'",
              enc_msg_len, enc_msg);
    return DN_ERROR;
  }
  dec_len += block_len;

  // EVP_CIPHER_CTX_cleanup(aesDecryptCtx);

  return (int)dec_len;
}

unsigned char *generate_aes_key(void) {
  if (RAND_bytes(aes_key, AES_KEYLEN) == 0) {
    return NULL;
  }
  aes_key[AES_KEYLEN] = '\0';

  return aes_key;
}

int dyn_rsa_size(void) { return rsa_size; }

rstatus_t dyn_rsa_encrypt(unsigned char *plain_msg,
                          unsigned char *encrypted_buf) {
  if (RSA_public_encrypt(AES_KEYLEN, plain_msg, encrypted_buf, rsa,
                         RSA_PKCS1_OAEP_PADDING) != RSA_size(rsa)) {
#if OPENSSL_VERSION_NUMBER < 0x10100000L
    ERR_load_crypto_strings();
#endif
    char err[130];
    ERR_error_string(ERR_get_error(), err);
    log_debug(LOG_VERB, "Error in encrypting message: %s\n", err);
    return DN_ERROR;
  }
  return RSA_size(rsa);
}

rstatus_t dyn_rsa_decrypt(unsigned char *encrypted_msg,
                          unsigned char *decrypted_buf) {
  if (RSA_private_decrypt(RSA_size(rsa), encrypted_msg, decrypted_buf, rsa,
                          RSA_PKCS1_OAEP_PADDING) != AES_KEYLEN) {
#if OPENSSL_VERSION_NUMBER < 0x10100000L
    ERR_load_crypto_strings();
#endif
    char err[130];
    ERR_error_string(ERR_get_error(), err);
    log_debug(LOG_VERB, "Error in decrypting message: %s\n", err);
    return DN_ERROR;
  }

  return AES_KEYLEN;
}
