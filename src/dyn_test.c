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

#define TEST_CONF_PATH        "conf/dynomite.yml"

#define TEST_LOG_DEFAULT       LOG_PVERB
#define TEST_LOG_PATH          NULL
#define TEST_MBUF_SIZE         512
#define TEST_ALLOCS_MSGS	   200000


static int show_help;
static int test_conf;

static char *data = "$2014$ 1 3 0 1 1 *1 d *0\r\n*3\r\n$3\r\nset\r\n$4\r\nfoo1\r\n$4\r\nbar1\r\n"
                    "$2014$ 2 3 0 1 1 *1 d *0\r\n*3\r\n$3\r\nset\r\n$4\r\nfoo2\r\n$413\r\nbar01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567892222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222222\r\n"
                    "$2014$ 3 3 0 1 1 *1 d *0\r\n*3\r\n$3\r\nset\r\n$4\r\nfoo3\r\n$4\r\nbar3\r\n";

static size_t position = 0;
static size_t test_mbuf_chunk_size;

static unsigned char aes_key[AES_KEYLEN];


static struct option long_options[] = {
    { "help",           no_argument,        NULL,   'h' },
    { "version",        no_argument,        NULL,   'V' },
    { "test-conf",      no_argument,        NULL,   't' },
    { "describe-stats", no_argument,        NULL,   'D' },
    { "gossip",         no_argument,        NULL,   'g' },
    { "verbose",        required_argument,  NULL,   'v' },
    { "output",         required_argument,  NULL,   'o' },
    { "conf-file",      required_argument,  NULL,   'c' },
    { "stats-port",     required_argument,  NULL,   's' },
    { "stats-interval", required_argument,  NULL,   'i' },
    { "stats-addr",     required_argument,  NULL,   'a' },
    { "pid-file",       required_argument,  NULL,   'p' },
    { "mbuf-size",      required_argument,  NULL,   'm' },
    { "max_msgs",       required_argument,  NULL,   'M' },
    { NULL,             0,                  NULL,    0  }
};

static char short_options[] = "hVtDgv:o:c:s:i:a:p:m:M";


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
        "  -m, --mbuf-size=N      : set size of mbuf chunk in bytes (default: %d bytes)" CRLF
        "  -M, --max-msgs=N       : set max number of messages to allocate (default: %d)" CRLF
        "",
        TEST_LOG_DEFAULT, TEST_LOG_DEFAULT, TEST_LOG_DEFAULT,
        TEST_LOG_PATH != NULL ? TEST_LOG_PATH : "stderr",
        TEST_CONF_PATH,
        TEST_MBUF_SIZE,
		TEST_ALLOCS_MSGS);
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

    nci->mbuf_chunk_size = TEST_MBUF_SIZE;
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

        case 'm':
            value = dn_atoi(optarg, strlen(optarg));
            if (value <= 0) {
                log_stderr("test: option -m requires a non-zero number");
                return DN_ERROR;
            }

            nci->mbuf_chunk_size = (size_t)value;
            break;

        case 'M':
            value = dn_atoi(optarg, strlen(optarg));
            if (value <= 0) {
                log_stderr("test: option -M requires a non-zero number");
                return DN_ERROR;
            }

            nci->alloc_msgs_max = (size_t)value;
            break;

        case '?':
            switch (optopt) {
            case 'o':
            case 'c':
            case 'p':
                log_stderr("test: option -%c requires a file name",
                           optopt);
                break;

            case 'm':
            case 'M':
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
init_server(struct server *s)
{
    s->idx = 0;
    s->owner = NULL;

    struct string pname = string("127.0.0.1:8102");
    string_copy(&s->pname, pname.data, pname.len);

    struct string name = string("127.0.0.1");
    string_copy(&s->name, name.data, name.len);

    s->state = UNKNOWN;

    s->port = (uint16_t)8102;
    s->weight = (uint32_t)1;

    struct string rack = string("rack1");
    string_copy(&s->rack, rack.data, rack.len);

    struct string dc = string("dc1");
    string_copy(&s->dc, dc.data, dc.len);

    s->is_local = false;
    //TODO-need to init tokens
    //s->tokens = cseed->tokens;

    struct sockinfo *info = malloc(sizeof(struct sockinfo));

    memset(info, 0, sizeof(*info));
    dn_resolve(&name, s->port, info);

    s->family = info->family;
    s->addrlen = info->addrlen;
    s->addr = (struct sockaddr *)&info->addr;


    s->ns_conn_q = 0;
    TAILQ_INIT(&s->s_conn_q);

    s->next_retry = 0LL;
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

            nmsg = msg_get(msg->owner, msg->request, conn->data_store, __FUNCTION__);
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
        log_debug(LOG_VERB, "AES key           : %s \n", base64_encode(msg, AES_KEYLEN));


        dyn_rsa_encrypt(msg, encrypted_buf);

        dyn_rsa_decrypt(encrypted_buf, decrypted_buf);

        log_debug(LOG_VERB, "Decrypted message : %s \n", base64_encode(decrypted_buf, AES_KEYLEN));
    }

    return DN_OK;
}


static rstatus_t
aes_test(void)
{
    log_debug(LOG_VERB, "aesKey is %s\n",
              base64_encode(aes_key, strlen((char*)aes_key)));

    const char *msg0 = "01234567890123";
    const char *msg1 = "0123456789012345678901234567890123456789012345";
    const char *msg2 = "01234567890123456789012345678901234567890123456789012345678901";
    const char *msg3 = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345601234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567";
    const char *msg4 = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345601234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";
    const char *msg5 = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345601234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

    const char *msgs[6];

    msgs[0] = msg0;
    msgs[1] = msg1;
    msgs[2] = msg2;
    msgs[3] = msg3;
    msgs[4] = msg4;
    msgs[5] = msg5;

    unsigned char *enc_msg = NULL;
    char *dec_msg          = NULL;
    size_t enc_msg_len;
    int dec_msg_len;

    loga("=======================AES======================");
    int i=0;
    int count = 6;
    for(;i<count;i++) {
        const char *msg = msgs[i];
        log_debug(LOG_VERB, "Message to AES encrypt: %s \n", msg);

        size_t expected_output_len = 16*((strlen(msg)/16) + 1);
        //loga("expected_output_len  = %lu", expected_output_len);
        // Encrypt the message with AES
        rstatus_t ret = DN_OK;
        ret = aes_encrypt((const unsigned char*)msg, strlen(msg)+1, &enc_msg, aes_key);
        if (ret == DN_ERROR) {
            log_debug(LOG_VERB, "AES encryption failed\n");
            return ret;
        }
        enc_msg_len = (size_t)ret;/* if success, aes_encrypt returns len */

        loga("enc_msg_len length %lu: ", enc_msg_len);
        if (enc_msg_len != expected_output_len) {
            return DN_ERROR;
        }
        // Print the encrypted message as a base64 string
        char *b64_string = base64_encode((unsigned char*)enc_msg, enc_msg_len);
        log_debug(LOG_VERB, "AES Encrypted message (base64): %s\n", b64_string);

        // Decrypt the message

        ret = aes_decrypt((unsigned char*)enc_msg, enc_msg_len, (unsigned char**) &dec_msg, aes_key);
        if(ret == DN_ERROR) {
            log_debug(LOG_VERB, "AES decryption failed\n");
            return ret;
        }
        dec_msg_len = ret; /* if success aes_decrypt returns len */

        log_debug(LOG_VERB, "%lu bytes decrypted\n", dec_msg_len);
        log_debug(LOG_VERB, "AES Decrypted message: %s\n", dec_msg);

        free(enc_msg);
        free(dec_msg);
        free(b64_string);
    }
    return DN_OK;
}

/* Inspection test */
static rstatus_t
aes_msg_test(struct server *server)
{
    //unsigned char* aes_key = generate_aes_key();
    struct conn *conn = conn_get_peer(server, false, true);
    struct msg *msg = msg_get(conn, true, conn->data_store, __FUNCTION__);

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
aes_msg_test2(struct server *server)
{
    unsigned char* aes_key = generate_aes_key();
    struct conn *conn = conn_get_peer(server, false, true);
    struct msg *msg = msg_get(conn, true, conn->redis);

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

    test_mbuf_chunk_size = nci.mbuf_chunk_size;
    position = 0;
    mbuf_init(&nci);
    msg_init(&nci);
    conn_init();

    crypto_init_for_test();
}

int
main(int argc, char **argv)
{
    //rstatus_t status;
    init_test(argc, argv);

    struct server *server = malloc(sizeof(struct server));
    init_server(server);

    struct conn *conn = conn_get_peer(server, false, true);
    struct msg *msg = msg_get(conn, true, conn->data_store, __FUNCTION__);

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

    ret = aes_msg_test(server);
    if (ret != DN_OK) {
        loga("Error in testing aes_msg_test !!!");
        goto err_out;
    }

    loga("Testing is done!!!");
err_out:
    return ret;
}
