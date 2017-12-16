#include <stdio.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <netdb.h>
#include <arpa/nameser.h>
#include <resolv.h>
#ifdef __APPLE__
#include <arpa/nameser_compat.h>
#endif

#include "dyn_seeds_provider.h"
#include "dyn_core.h"
#include "dyn_string.h"

// Keep poling DNS server for the TXT record with seeds, same format as for Florida seeds
//
//
// DYNOMITE_DNS_TXT_NAME=_dynomite.yourhost.com src/dynomite -c conf/dynomite_dns_single.yml -v 11
//
// To compile the domain use make CFLAGS="-DDNS_TXT_NAME=_dynomite.yourhost.com"
//
//


#ifndef DNS_TXT_NAME
#define DNS_TXT_NAME "_dynomite.ec2-internal"
#endif

static char * dnsName  = NULL;
static char * dnsType  = NULL;
static int queryType = T_TXT;
static int64_t last = 0; //storing last time for seeds check
static uint32_t last_seeds_hash = 0;

static bool seeds_check()
{
    int64_t now = dn_msec_now();

    int64_t delta = (int64_t)(now - last);
    log_debug(LOG_VERB, "Delta or elapsed time : %lu", delta);
    log_debug(LOG_VERB, "Seeds check internal %d", SEEDS_CHECK_INTERVAL);

    if (delta > SEEDS_CHECK_INTERVAL) {
        last = now;
        return true;
    }

    return false;
}


static uint32_t
hash_seeds(uint8_t *seeds, size_t length)
{
    const uint8_t *ptr = seeds;
    uint32_t value = 0;

    while (length--) {
        uint32_t val = (uint32_t) *ptr++;
        value += val;
        value += (value << 10);
        value ^= (value >> 6);
    }
    value += (value << 3);
    value ^= (value >> 11);
    value += (value << 15);

    return value;
}

uint8_t
dns_get_seeds(struct context * ctx, struct mbuf *seeds_buf) 
{
    static int _env_checked = 0;

    if (!_env_checked) {
        _env_checked = 1;
        dnsName = getenv("DYNOMITE_DNS_NAME");
        if (dnsName == NULL)  dnsName = DNS_TXT_NAME;
        dnsType = getenv("DYNOMITE_DNS_TYPE");
        if (dnsType != NULL) { if (strcmp(dnsType, "A") == 0) queryType = T_A; }
    }

    log_debug(LOG_VVERB, "checking for %s", dnsName);

    if (!seeds_check()) {
        return DN_NOOPS;
    }

    unsigned char buf[BUFSIZ];

    int r = res_query(dnsName, C_IN, queryType, buf, sizeof(buf));
    if (r == -1) {
        log_debug(LOG_DEBUG, "DNS response for %s: %s", dnsName, hstrerror(h_errno));
        return DN_NOOPS;
    }
    if (r >= sizeof(buf)) {
        log_debug(LOG_DEBUG, "DNS reply is too large for %s: %d, bufsize: %d", dnsName, r, sizeof(buf));
        return DN_NOOPS;
    }
    HEADER *hdr = (HEADER*)buf;
    if (hdr->rcode != NOERROR) {
        log_debug(LOG_DEBUG, "DNS reply code for %s: %d", dnsName, hdr->rcode);
        return DN_NOOPS;
    }
    int na = ntohs(hdr->ancount);

    ns_msg m;
    if (ns_initparse(buf, r, &m) == -1) {
        log_debug(LOG_DEBUG, "ns_initparse error for %s: %s", dnsName, strerror(errno));
        return DN_NOOPS;
    }
    int i;
    ns_rr rr;
    for (i = 0; i < na; ++i) {
        if (ns_parserr(&m, ns_s_an, i, &rr) == -1) {
            log_debug(LOG_DEBUG, "ns_parserr for %s: %s", dnsName, strerror (errno));
            return DN_NOOPS;
        }
        mbuf_rewind(seeds_buf);
        unsigned char *s = ns_rr_rdata(rr);
        if (s[0] >= ns_rr_rdlen(rr)) {
            log_debug(LOG_DEBUG, "invalid length for %s: %d < %d", dnsName, s[0], ns_rr_rdlen(rr));
            return DN_NOOPS;
        }
        log_debug(LOG_VERB, "seeds for %s: %.*s", dnsName, s[0], s +1);
        mbuf_copy(seeds_buf, s + 1, s[0]);
    }

    uint32_t seeds_hash = hash_seeds(seeds_buf->pos, mbuf_length(seeds_buf));
    if (last_seeds_hash != seeds_hash) {
        last_seeds_hash = seeds_hash;
    } else {
        return DN_NOOPS;
    }
    return DN_OK;
}


