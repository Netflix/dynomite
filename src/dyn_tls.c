/*
 * dyn_tls.c
 *
 * Copyright (C) 2014 Netflix, Inc.
 */

#include "dyn_core.h"
#include "dyn_tls.h"
#include "dyn_log.h"
#include "errno.h"

//static const char* DYN_DEFAULT_CIPHER =  "AES128-GCM-SHA256";
static const char* DYN_DEFAULT_CIPHER = "AES128-GCM-SHA256";


rstatus_t
dyn_tls_init(struct tls_ctx *t) {
    log_debug(LOG_INFO,"Initializing tls context");
    t->meth = TLSv1_2_server_method();
    //t->meth = TLSv1_2_method();
    t->ssl_ctx = SSL_CTX_new(t->meth);

    if (!t->ssl_ctx) {
        log_error("Unable to create ssl context");
        ERR_print_errors_fp(getLogger()->fp);
        return TLS_ERR_CTX;
    }

    /* Cipher AES256-GCM-SHA384 - good performance with AES-NI support. */
    if (!SSL_CTX_set_cipher_list(t->ssl_ctx, DYN_DEFAULT_CIPHER)) {
        log_error("Could not set cipher.");
        return TLS_ERR_SET_CIPHER;
    }
    else {
        log_debug(LOG_INFO, "Cipher set to %s", DYN_DEFAULT_CIPHER);
    }

    /* Disable compression to help prevent vulnerabilities. */
    if (!SSL_CTX_set_options(t->ssl_ctx, SSL_OP_NO_COMPRESSION)) {
        log_error("Could not disable compression.");
        return TLS_ERR_SET_COMPRESSION;
    }
    else {
        log_debug(LOG_INFO, "TLS Compression disabled.");
    }

    /* Configure certificates and keys */
    if (SSL_CTX_load_verify_locations(t->ssl_ctx, CERTF, 0) <= 0) {
        log_error("Could not load certificates");
        ERR_print_errors_fp(getLogger()->fp);
        return TLS_ERR_LOAD_CERT;
    }

    if (SSL_CTX_use_certificate_file(t->ssl_ctx, CERTF, SSL_FILETYPE_PEM) <= 0) {
        log_error("Cannot use certificate.");
        ERR_print_errors_fp(getLogger()->fp);
        return TLS_ERR_USE_CERT;
    }
    else {
        log_debug(LOG_INFO, "Loading cert file: %s", CERTF);
    }


    if (SSL_CTX_use_PrivateKey_file(t->ssl_ctx, KEYF, SSL_FILETYPE_PEM) <= 0) {
        log_error("Cannot use key");
        ERR_print_errors_fp(getLogger()->fp);
        return TLS_ERR_USE_KEY;
    }
    else {
        log_debug(LOG_INFO, "Loading key file: %s", KEYF);
    }


    if (!SSL_CTX_check_private_key(t->ssl_ctx)) {
        log_error("Private key does not match public key in certificate.\n");
        return TLS_ERR_CHECK_KEY;
    }

    /* Enable client certificate verification. Enable before accepting connections. */
    /*SSL_CTX_set_verify(t->ssl_ctx,
            SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT | SSL_VERIFY_CLIENT_ONCE,
            0);*/

    return DN_OK;
}


rstatus_t
dyn_tls_client_init(struct tls_ctx *t) {
    log_debug(LOG_INFO,"Initializing client tls context");
    t->meth = TLSv1_2_client_method();
    t->ssl_ctx = SSL_CTX_new(t->meth);

    if (!t->ssl_ctx) {
        log_error("Unable to create ssl context");
        ERR_print_errors_fp(getLogger()->fp);
        return TLS_ERR_CTX;
    }

    /* Cipher AES256-GCM-SHA384 - good performance with AES-NI support. */
    if (!SSL_CTX_set_cipher_list(t->ssl_ctx, DYN_DEFAULT_CIPHER)) {
        log_error("Could not set cipher.");
        return TLS_ERR_SET_CIPHER;
    }
    else {
        log_debug(LOG_INFO, "Cipher set to %s", DYN_DEFAULT_CIPHER);
    }

    /* Disable compression to help prevent vulnerabilities. */
    if (!SSL_CTX_set_options(t->ssl_ctx, SSL_OP_NO_COMPRESSION)) {
        log_error("Could not disable compression.");
        return TLS_ERR_SET_COMPRESSION;
    }
    else {
        log_debug(LOG_INFO, "TLS Compression disabled.");
    }

    /* Configure certificates and keys */
    if (SSL_CTX_load_verify_locations(t->ssl_ctx, CERTF, 0) <= 0) {
        log_error("Could not load certificates");
        ERR_print_errors_fp(getLogger()->fp);
        return TLS_ERR_LOAD_CERT;
    }

    if (SSL_CTX_use_certificate_file(t->ssl_ctx, CERTF, SSL_FILETYPE_PEM) <= 0) {
        log_error("Cannot use certificate.");
        ERR_print_errors_fp(getLogger()->fp);
        return TLS_ERR_USE_CERT;
    }
    else {
        log_debug(LOG_INFO, "Loading cert file: %s", CERTF);
    }


    if (SSL_CTX_use_PrivateKey_file(t->ssl_ctx, KEYF, SSL_FILETYPE_PEM) <= 0) {
        log_error("Cannot use key");
        ERR_print_errors_fp(getLogger()->fp);
        return TLS_ERR_USE_KEY;
    }
    else {
        log_debug(LOG_INFO, "Loading key file: %s", KEYF);
    }


    if (!SSL_CTX_check_private_key(t->ssl_ctx)) {
        log_error("Private key does not match public key in certificate.\n");
        return TLS_ERR_CHECK_KEY;
    }

    /* Enable client certificate verification. Enable before accepting connections. */
    /*SSL_CTX_set_verify(t->ssl_ctx,
            SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT | SSL_VERIFY_CLIENT_ONCE,
            0);*/

    return DN_OK;
}


void
dyn_tls_deinit(struct conn *c) {
    log_debug(LOG_INFO, "Closing secure connection with sd %d.", c->sd);
    SSL_shutdown(c->ssl);
    SSL_free(c->ssl);
    if (c->tls_ctx != NULL) {
        SSL_CTX_free(c->tls_ctx->ssl_ctx);
        dn_free(c->tls_ctx);
    }
}


void dyn_tls_close(struct conn *c) {
    log_debug(LOG_INFO, "Closing secure connection with sd %d.", c->sd);
    SSL_free(c->ssl);
}


static void
dump_cert_info(SSL *ssl, bool server) {

    if(server) {
        log_debug(LOG_INFO, "dyn ssl server version: %s", SSL_get_version(ssl));
    }
    else {
        log_debug(LOG_INFO, "Client Version: %s", SSL_get_version(ssl));
    }

    /* The cipher negotiated and being used */
    log_debug(LOG_INFO, "Using cipher %s", SSL_get_cipher(ssl));

    /* Get client's certificate (note: beware of dynamic allocation) - opt */
    X509 *client_cert = SSL_get_peer_certificate(ssl);
    if (client_cert != NULL) {
        if(server) {
        log_debug(LOG_INFO, "Client certificate:\n");
        }
        else {
            log_debug(LOG_INFO, "Server certificate:\n");
        }
        char *str = X509_NAME_oneline(X509_get_subject_name(client_cert), 0, 0);
        if(str == NULL) {
            log_warn("X509 subject name is null");
        }
        log_debug(LOG_INFO, "\t Subject: %s\n", str);
        OPENSSL_free(str);

        str = X509_NAME_oneline(X509_get_issuer_name(client_cert), 0, 0);
        if(str == NULL) {
            log_warn("X509 issuer name is null");
        }
        log_debug(LOG_INFO, "\t Issuer: %s\n", str);
        OPENSSL_free(str);

        /* Deallocate certificate, free memory */
        X509_free(client_cert);
    } else {
        log_debug(LOG_INFO, "Client does not have certificate.\n");
    }
}

rstatus_t
dyn_tls_accept(struct conn *s,  /* tls server */
           struct conn *c,  /* tls client*/
           int sd          /*accepted*/) {
    SSL* ssl;

    ssl = SSL_new(s->tls_ctx->ssl_ctx);
    if(!ssl) {
        log_error("Could not create SSL");
        return DN_ERROR;
    }

    c->tls_ctx = s->tls_ctx;
    c->ssl = ssl;

    SSL_set_fd(ssl, sd);
    SSL_set_accept_state(ssl);

    for(;;) {
        int success = SSL_accept(ssl);

        if(success) {
            log_debug(LOG_INFO, "SSL accept on sd %d", sd);
            dump_cert_info(ssl, true);
            return DN_OK;
        }
        else if (success == 0) {
            log_debug(LOG_INFO, "Handshake was unsuccessful but shutdown was controlled.");
            return DN_ERROR;
        }

        int32_t err = SSL_get_error(c->ssl, success);

        /* For non-blocking operation did not complete. Try again later. */
        if (err == SSL_ERROR_WANT_READ             ||
                err == SSL_ERROR_WANT_WRITE        ||
                err == SSL_ERROR_WANT_X509_LOOKUP  ||
                err == SSL_ERROR_WANT_CONNECT      ||
                err == SSL_ERROR_WANT_ACCEPT) {
            continue;
        }
        else {
            log_error("Error SSL_accept no: %d err str:", err, strerror(err));
            SSL_free(ssl);
            return DN_ERROR;
        }
    }
}

rstatus_t
dyn_tls_connect(struct conn *p_conn,  /* tls dnode peer connect */
           int sd) {
    SSL* ssl;

    loga("In dyn_tls_connect ...........................................");
    ssl = SSL_new(p_conn->tls_ctx->ssl_ctx);
    if(!ssl) {
        log_error("Could not create SSL");
        return DN_ERROR;
    }

    p_conn->ssl = ssl;

    SSL_set_fd(ssl, sd);
    SSL_set_connect_state(ssl);

    for(;;) {
		int success = SSL_connect(ssl);

		if(success) {
			loga("connecttttttttttttttttttttttttttttttttt");
			dump_cert_info(ssl, false);
			return DN_OK;
		}

		int32_t err = SSL_get_error(p_conn->ssl, success);

		/* For non-blocking operation did not complete. Try again later. */
		if (err == SSL_ERROR_WANT_READ        ||
		    err == SSL_ERROR_WANT_WRITE       ||
		    err == SSL_ERROR_WANT_X509_LOOKUP ||
		    err == SSL_ERROR_WANT_CONNECT) {
		            return DN_OK;
		}
        else {
        	if(err == SSL_ERROR_ZERO_RETURN) {
        	    log_error("SSL_connect: close notify received from peer");
        	}
			log_error("Error SSL_accept: %d", err);
			SSL_free(ssl);
			return DN_ERROR;
		}
    }
}

ssize_t
dyn_ssl_read(SSL *ssl,void *buf,int num) {
    int success = SSL_read(ssl, buf, num);

    if (success > 0) {
        return success;
    }

    int32_t err = SSL_get_error(ssl, success);

    if(err == SSL_ERROR_WANT_READ   ||
            err == SSL_ERROR_WANT_WRITE ||
            err == SSL_ERROR_WANT_X509_LOOKUP) {
            errno = EAGAIN;
            return (ssize_t) DN_EAGAIN;
    }
    else if(err == SSL_ERROR_ZERO_RETURN) {
        log_debug(LOG_INFO, "SSL_read close notify received from peer");
        return DN_OK;
    }
    else {
        log_error("SSL read error: %s", strerror(err));
        return (ssize_t) DN_ERROR;
    }
}

ssize_t
dyn_ssl_write1(SSL *ssl,const void *buf,int num) {
    int success = SSL_write(ssl, buf, num);

    loga("successssssssssssssssssssssssssss %d", success);
    if (success > 0) {
        return success;
    }

    int32_t err = SSL_get_error(ssl, success);

    if(err == SSL_ERROR_WANT_READ ||
        err == SSL_ERROR_WANT_WRITE) {
        errno = EAGAIN;
        return (ssize_t) DN_EAGAIN;
    }
    else if(err == SSL_ERROR_ZERO_RETURN) {
        log_debug(LOG_INFO, "SSL_write close notify received from peer");
        return DN_OK;
    }
    else {
        log_error("SSL write error: %s", strerror(err));
        return (ssize_t) DN_ERROR;
    }
}


ssize_t
dyn_ssl_write(SSL *ssl,const void *buf,int num) {

		int success = SSL_write(ssl, buf, num);

		loga("successssssssssssssssssssssssssss %d", success);
		if (success > 0) {
			return success;
		}

		int32_t err = SSL_get_error(ssl, success);

		if(err == SSL_ERROR_WANT_READ ||
           err == SSL_ERROR_WANT_WRITE ||
           err == SSL_ERROR_WANT_X509_LOOKUP) {
           errno = EAGAIN;
           //continue;
           return (ssize_t) DN_EAGAIN;
		}
		else if(err == SSL_ERROR_ZERO_RETURN) {
			log_debug(LOG_INFO, "SSL_write close notify received from peer");
			return DN_OK;
		}
		else {
			log_error("SSL write error: %s", strerror(err));
			return (ssize_t) DN_ERROR;
		}


	return (ssize_t) DN_EAGAIN;
}
