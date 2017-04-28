/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2015 Netflix, Inc.
 */ 


#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <getopt.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/utsname.h>

#include "dyn_core.h"
#include "dyn_conf.h"
#include "dyn_signal.h"
#include "dyn_dnode_peer.h"

#define TEST_CONF_PATH                 "conf/dynomite.yml"

#define TEST_LOG_DEFAULT               LOG_PVERB
#define TEST_LOG_PATH                  NULL

#define TEST_MBUF_SIZE                 16384
#define TEST_ALLOC_MSGS_MAX            300000

static int show_help;
static int test_conf;


static char *data = "$2014$ 1 3 0 1 1 *1 d *0\r\n*3\r\n$3\r\nset\r\n$4\r\nfoo1\r\n$4\r\nbar1\r\n"
                    "$2014$ 2 3 0 1 1 *1 d *0\r\n*3\r\n$3\r\nset\r\n$4\r\nfoo2\r\n$413\r\nbar01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567892222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222\r\n"
                    "$2014$ 3 3 0 1 1 *1 d *0\r\n*3\r\n$3\r\nset\r\n$4\r\nfoo3\r\n$4\r\nbar3\r\n";

static size_t position = 0;

static struct option long_options[] = {
    { "help",           no_argument,        NULL,   'h' },
    { "version",        no_argument,        NULL,   'V' },
    { "test-conf",      no_argument,        NULL,   't' },
    { "describe-stats", no_argument,        NULL,   'D' },
    { "verbose",        required_argument,  NULL,   'v' },
    { "output",         required_argument,  NULL,   'o' },
    { "conf-file",      required_argument,  NULL,   'c' },
    { "pid-file",       required_argument,  NULL,   'p' },
    { NULL,             0,                  NULL,    0  }
};

static char short_options[] = "hVtDgv:o:c:s:i:a:p";


static void
dn_show_usage(void)
{
    log_stderr(
        "Usage: test [-?hVdDt] [-v verbosity level] [-o output file]" CRLF
        "                  [-c conf file] [-m mbuf size] [-M max alloc messages]" CRLF
        "");
    log_stderr(
        "Options:" CRLF
        "  -h, --help             : this help" CRLF
        "  -V, --version          : show version and exit" CRLF
        "  -t, --test-conf        : test configuration for syntax errors and exit");
    log_stderr(
        "  -v, --verbosity=N      : set logging level (default: %d, min: %d, max: %d)" CRLF
        "  -o, --output=S         : set logging file (default: %s)" CRLF
        "  -c, --conf-file=S      : set configuration file (default: %s)" CRLF
        "",
        TEST_LOG_DEFAULT, TEST_LOG_DEFAULT, TEST_LOG_DEFAULT,
        TEST_LOG_PATH != NULL ? TEST_LOG_PATH : "stderr",
        TEST_CONF_PATH);
}


static rstatus_t
test_pre_run(struct instance *nci)
{
    rstatus_t status;

    status = log_init(nci->log_level, nci->log_filename);
    if (status != DN_OK) {
        return status;
    }

    status = signal_init();
    if (status != DN_OK) {
        return status;
    }

    return DN_OK;
}


static void
test_set_default_options(struct instance *nci)
{
    int status;

    nci->ctx = NULL;
    nci->log_level = TEST_LOG_DEFAULT;
    nci->log_filename = TEST_LOG_PATH;
    nci->conf_filename = TEST_CONF_PATH;

    status = dn_gethostname(nci->hostname, DN_MAXHOSTNAMELEN);
    if (status < 0) {
        log_warn("gethostname failed, ignored: %s", strerror(errno));
        dn_snprintf(nci->hostname, DN_MAXHOSTNAMELEN, "unknown");
    }

    nci->hostname[DN_MAXHOSTNAMELEN - 1] = '\0';

}

static rstatus_t
test_get_options(int argc, char **argv, struct instance *nci)
{
    int c, value;

    opterr = 0;

    for (;;) {
        c = getopt_long(argc, argv, short_options, long_options, NULL);
        if (c == -1) {
            /* no more options */
            break;
        }

        switch (c) {
        case 'h':
            show_help = 1;
            break;

        case 't':
            test_conf = 1;
            nci->log_level = 11;
            break;

        case 'v':
            value = dn_atoi(optarg, strlen(optarg));
            if (value < 0) {
                log_stderr("test: option -v requires a number");
                return DN_ERROR;
            }
            nci->log_level = value;
            break;

        case 'o':
            nci->log_filename = optarg;
            break;

        case 'c':
            nci->conf_filename = optarg;
            break;

        case '?':
            switch (optopt) {
            case 'o':
            case 'c':
            case 'p':
                log_stderr("test: option -%c requires a file name",
                           optopt);
                break;

            case 'v':
            case 's':
            case 'i':
                log_stderr("test: option -%c requires a number", optopt);
                break;

            case 'a':
                log_stderr("test: option -%c requires a string", optopt);
                break;

            default:
                log_stderr("test: invalid option -- '%c'", optopt);
                break;
            }
            return DN_ERROR;

        default:
            log_stderr("dynomite: invalid option -- '%c'", optopt);
            return DN_ERROR;

        }
    }

    return DN_OK;
}


static rstatus_t
init_peer(struct node *s)
{
    s->idx = 0;
    s->owner = NULL;

    struct string pname = string("127.0.0.1:8102");
    string_copy(&s->endpoint.pname, pname.data, pname.len);

    struct string name = string("127.0.0.1");
    string_copy(&s->name, name.data, name.len);

    s->state = UNKNOWN;

    s->endpoint.port = (uint16_t)8102;
    s->endpoint.weight = (uint32_t)1;

    struct string rack = string("rack1");
    string_copy(&s->rack, rack.data, rack.len);

    struct string dc = string("dc1");
    string_copy(&s->dc, dc.data, dc.len);

    s->is_local = false;
    //TODO-need to init tokens
    //s->tokens = cseed->tokens;

    struct sockinfo *info = malloc(sizeof(struct sockinfo));

    memset(info, 0, sizeof(*info));
    dn_resolve(&name, s->endpoint.port, info);

    s->endpoint.family = info->family;
    s->endpoint.addrlen = info->addrlen;
    s->endpoint.addr = (struct sockaddr *)&info->addr;

    s->conn = NULL;

    s->next_retry_ms = 0ULL;
    s->reconnect_backoff_sec = MIN_WAIT_BEFORE_RECONNECT_IN_SECS;
    s->failure_count = 0;

    s->processed = 0;
    s->is_seed = 1;
    s->is_secure = 0;

    log_debug(LOG_NOTICE, "Filling up server data");

    return DN_OK;
}


static size_t fill_buffer(struct mbuf *mbuf)
{
    loga("total data size: %d", dn_strlen(data));
    loga("mbuf size: %d", mbuf_size(mbuf));
    size_t data_size = dn_strlen(data) - position;

    loga("data left-over size: %d", data_size);
    if (data_size <= 0) {
        return 0;
    }

    size_t min_len = data_size > mbuf_size(mbuf) ? mbuf_size(mbuf) : data_size;
    mbuf_copy(mbuf, &data[position], min_len);
    position += min_len;

    return min_len;
}

static rstatus_t
test_msg_recv_chain(struct conn *conn, struct msg *msg)
{
    struct msg *nmsg;
    struct mbuf *mbuf, *nbuf;

    mbuf = STAILQ_LAST(&msg->mhdr, mbuf, next);

    mbuf = mbuf_get();
    mbuf_insert(&msg->mhdr, mbuf);
    msg->pos = mbuf->pos;

    ASSERT(mbuf->end - mbuf->last > 0);


    uint32_t data_n = (uint32_t)fill_buffer(mbuf);
    msg->mlen += data_n;


    loga("msg->mlen = %d", + msg->mlen);
    loga("mbuf_length = %d", mbuf_length(mbuf));


    bool is_done = false;

    for(;!is_done;) {
        msg->parser(msg);

        switch (msg->result) {
        case MSG_PARSE_OK:
            log_debug(LOG_VVERB, "Parsing MSG_PARSE_OK");
            if (msg->pos == mbuf->last) {
                log_debug(LOG_VVERB, "Parsing MSG_PARSE_OK - done - no more data to parse!");
                is_done = true;
            }

            nbuf = mbuf_split(&msg->mhdr, msg->pos, NULL, NULL);
            if (nbuf == NULL) {
                log_debug(LOG_VVERB, "Parsing MSG_PARSE_OK - more data but can't split!");
            }

            nmsg = msg_get(msg->owner, msg->is_request, __FUNCTION__);
            mbuf_insert(&nmsg->mhdr, nbuf);
            nmsg->pos = nbuf->pos;

            /* update length of current (msg) and new message (nmsg) */
            nmsg->mlen = mbuf_length(nbuf);
            msg->mlen -= nmsg->mlen;

            data_n = (uint32_t)fill_buffer(nbuf);
            nmsg->mlen += data_n;

            msg = nmsg;
            mbuf = nbuf;

            break;

        case MSG_PARSE_REPAIR:
            //status = msg_repair(ctx, conn, msg);
            log_debug(LOG_VVERB, "Parsing MSG_PARSE_REPAIR");
            msg = NULL;
            break;

        case MSG_PARSE_AGAIN:
            log_debug(LOG_VVERB, "Parsing MSG_PARSE_AGAIN");

            nbuf = mbuf_split(&msg->mhdr, msg->pos, NULL, NULL);
            mbuf_insert(&msg->mhdr, nbuf);
            msg->pos = nbuf->pos;
            data_n = (uint32_t)fill_buffer(nbuf);
            msg->mlen += data_n;
            mbuf = nbuf;

            break;

        default:
            log_debug(LOG_VVERB, "Parsing error in dyn_mode");
            msg = NULL;
            break;
        }

    }

    loga("Done parsing .........!");
    return DN_OK;
}


static rstatus_t
rsa_test(void)
{
    static unsigned char encrypted_buf[256];
    static unsigned char decrypted_buf[AES_KEYLEN + 1];
    static unsigned char *msg;

    int i=0;
    for(; i<3; i++) {
        msg = generate_aes_key();

        log_debug(LOG_VERB, "i = %d", i);
        SCOPED_CHARPTR(encoded_aes_key) = base64_encode(msg, AES_KEYLEN);
        log_debug(LOG_VERB, "AES key           : %s \n", encoded_aes_key);


        dyn_rsa_encrypt(msg, encrypted_buf);

        dyn_rsa_decrypt(encrypted_buf, decrypted_buf);

        SCOPED_CHARPTR(encoded_decrypted_buf) = base64_encode(decrypted_buf, AES_KEYLEN);
        log_debug(LOG_VERB, "Decrypted message : %s \n", encoded_decrypted_buf);
    }

    return DN_OK;
}

static void gen_random(unsigned char *s, const int len)
{
    static const unsigned char possible_data[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz\r\n";
    int i;
    for (i = 0; i < len; ++i) {
        s[i] = possible_data[rand() % (sizeof(possible_data) - 1)];
    }

    s[len] = 0;
}

#define MAX_MSG_LEN 512
static rstatus_t
aes_test(void)
{
    unsigned char msg[MAX_MSG_LEN+1];
    loga("=======================AES======================");
    unsigned char* aes_key = generate_aes_key();
    SCOPED_CHARPTR(aes_key_print) = base64_encode(aes_key, strlen((char*)aes_key));
    loga("aesKey is '%s'", aes_key_print);

    size_t i=0;
    size_t count = 10000000;
    loga("Running %lu encryption/decryption messages", count);
    for(;i<count;i++) {
        gen_random(msg, rand() % MAX_MSG_LEN);
        unsigned int msg_len = strlen(msg) + 1; // Also encrypt the \0 at the end

        // Encrypt the message with AES
        unsigned char *enc_msg = NULL;
        rstatus_t ret = aes_encrypt((const unsigned char*)msg, msg_len, &enc_msg,
                                    aes_key);
        if (ret == DN_ERROR) {
            log_panic("msg:'%s'\nencryption failed aes_key '%s'\n",
                      msg, aes_key_print);
            return ret;
        }
        size_t enc_msg_len = (size_t)ret;/* if success, aes_encrypt returns len */

        size_t expected_output_len = 16*(msg_len/16 + 1);
        if (enc_msg_len != expected_output_len) {
            log_panic("msg:'%s'\nexpected encrypted len: %lu encrypted len %lu\n\n",
                      msg, expected_output_len, enc_msg_len);
            return DN_ERROR;
        }

        // Decrypt the message
        char *dec_msg = NULL;
        ret = aes_decrypt((unsigned char*)enc_msg, enc_msg_len, (unsigned char**) &dec_msg, aes_key);
        ASSERT_LOG(ret != DN_ERROR,"msg '%s'\nencrypted msg:'%.*s'\ndecryption failed aes_key %s",
                   msg, enc_msg_len, enc_msg, aes_key_print);

        if (strcmp(msg, dec_msg) != 0) {
            loga_hexdump(msg, strlen(msg), "Original Message:");
            loga_hexdump(dec_msg, strlen(dec_msg), "Decrypted Message:");
            log_panic("encryption/Decryption mismatch");
        }

        free(enc_msg);
        free(dec_msg);
        if ((i+1)%1000000 == 0) {
            loga("Completed Running %lu messages", i+1);
        }
    }
    return DN_OK;
}

void
init_peer_conn(struct conn *conn)
{
    conn->dyn_mode = 1;
    conn->sd = 0;
}
/* Inspection test */
static rstatus_t
aes_msg_test(struct node *server)
{
    //unsigned char* aes_key = generate_aes_key();
    struct conn *conn = conn_get(server, init_peer_conn);
    struct msg *msg = msg_get(conn, true, __FUNCTION__);

    struct mbuf *mbuf1 = mbuf_get();
    struct string s1 = string("abc");
    mbuf_write_string(mbuf1, &s1);
    STAILQ_INSERT_HEAD(&msg->mhdr, mbuf1, next);

    struct mbuf *mbuf2 = mbuf_get();
    struct string s2 = string("abcabc");
    mbuf_write_string(mbuf2, &s2);
    STAILQ_INSERT_TAIL(&msg->mhdr, mbuf2, next);

    /*
    loga("dumping the content of the original msg: ");
    msg_dump(msg);

    dyn_aes_encrypt_msg(msg, aes_key);

    loga("dumping the content of encrypted msg");
    msg_dump(msg);

    dyn_aes_decrypt_msg(msg, aes_key);

    loga("dumping the content of decrytped msg");
    msg_dump(msg);
    */

    return DN_OK;
}

/*

static rstatus_t
aes_msg_test2(struct node *server)
{
    unsigned char* aes_key = generate_aes_key();
    struct conn *conn = conn_get_peer(server, false, true);
    struct msg *msg = msg_get(conn, true, __FUNCTION__);

    struct mbuf *mbuf1 = mbuf_get();
    mbuf_write_bytes(mbuf1, data, mbuf_size(mbuf1));
    STAILQ_INSERT_HEAD(&msg->mhdr, mbuf1, next);

    struct mbuf *mbuf2 = mbuf_get();
    mbuf_write_bytes(mbuf2, data + mbuf_size(mbuf2), strlen(data) - mbuf_size(mbuf2));
    STAILQ_INSERT_TAIL(&msg->mhdr, mbuf2, next);

    loga("dumping the content of the original msg: ");
    msg_dump(msg);

    dyn_aes_encrypt_msg(msg, aes_key);

    loga("dumping the content of msg after encrypting it: ");
    msg_dump(msg);

    dyn_aes_decrypt_msg(msg, aes_key);

    loga("dumping the content after decrypting it: ");
    msg_dump(msg);

    return DN_OK;
}
*/

static void
test_core_ctx_create(struct instance *nci)
{
    struct context *ctx;

    srand((unsigned) time(NULL));

    ctx = dn_alloc(sizeof(*ctx));
    if (ctx == NULL) {
        loga("Failed to create context!!!");
    }
    nci->ctx = ctx;
    ctx->instance = nci;
    ctx->cf = NULL;
    ctx->stats = NULL;
    ctx->evb = NULL;
    ctx->dyn_state = INIT;
}

/**
 * This is very primitive
 */
static void
test_server_pool(struct instance *nci)
{
    struct context *ctx = nci->ctx;
    struct server_pool *sp = &ctx->pool;
	sp->mbuf_size = TEST_MBUF_SIZE;
	sp->alloc_msgs_max = TEST_ALLOC_MSGS_MAX;

    mbuf_init(sp->mbuf_size);
    msg_init(sp->alloc_msgs_max);
}

static void
init_test(int argc, char **argv)
{
    rstatus_t status;
    struct instance nci;


    test_set_default_options(&nci);

    status = test_get_options(argc, argv, &nci);
    if (status != DN_OK) {
        dn_show_usage();
        exit(1);
    }

    test_pre_run(&nci);

    test_core_ctx_create(&nci);
    position = 0;
    conn_init();

    test_server_pool(&nci);


    crypto_init_for_test();
}

int
main(int argc, char **argv)
{
    //rstatus_t status;
    init_test(argc, argv);

    struct node *peer = malloc(sizeof(struct node));
    memset(peer, 0, sizeof(struct node));
    init_peer(peer);

    struct conn *conn = conn_get(peer, init_peer_conn);
    struct msg *msg = msg_get(conn, true, __FUNCTION__);

    //test payload larger than mbuf_size
    rstatus_t ret = DN_OK;
    ret = test_msg_recv_chain(conn, msg);
    if (ret != DN_OK) {
        loga("Error in testing msg_recv_chain!!!");
        goto err_out;
    }
    ret = rsa_test();
    if (ret != DN_OK) {
        loga("Error in testing RSA !!!");
        goto err_out;
    }

    ret = aes_test();
    if (ret != DN_OK) {
        loga("Error in testing AES !!!");
        goto err_out;
    }

    ret = aes_msg_test(peer);
    if (ret != DN_OK) {
        loga("Error in testing aes_msg_test !!!");
        goto err_out;
    }

    loga("Testing is done!!!");
err_out:
    return ret;
}
