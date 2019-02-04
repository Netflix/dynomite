/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed
 * storages. Copyright (C) 2014 Netflix, Inc.
 */

/*
 * twemproxy - A fast and lightweight proxy for memcached protocol.
 * Copyright (C) 2011 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _DYN_UTIL_H_
#define _DYN_UTIL_H_

#include <netinet/in.h>
#include <stdarg.h>
#include <stdbool.h>
#include <sys/un.h>
#include <unistd.h>

#define LF (uint8_t)10
#define CR (uint8_t)13
#define CRLF "\x0d\x0a"
#define CRLF_LEN (sizeof("\x0d\x0a") - 1)

#define NELEMS(a) ((sizeof(a)) / sizeof((a)[0]))

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

#define SQUARE(d) ((d) * (d))
#define VAR(s, s2, n) (((n) < 2) ? 0.0 : ((s2)-SQUARE(s) / (n)) / ((n)-1))
#define STDDEV(s, s2, n) (((n) < 2) ? 0.0 : sqrt(VAR((s), (s2), (n))))

#define DN_INET4_ADDRSTRLEN (sizeof("255.255.255.255") - 1)
#define DN_INET6_ADDRSTRLEN \
  (sizeof("ffff:ffff:ffff:ffff:ffff:ffff:255.255.255.255") - 1)
#define DN_INET_ADDRSTRLEN MAX(DN_INET4_ADDRSTRLEN, DN_INET6_ADDRSTRLEN)
#define DN_UNIX_ADDRSTRLEN \
  (sizeof(struct sockaddr_un) - offsetof(struct sockaddr_un, sun_path))

#define DN_MAXHOSTNAMELEN 256

/*
 * Length of 1 byte, 2 bytes, 4 bytes, 8 bytes and largest integral
 * type (uintmax_t) in ascii, including the null terminator '\0'
 *
 * From stdint.h, we have:
 * # define UINT8_MAX	(255)
 * # define UINT16_MAX	(65535)
 * # define UINT32_MAX	(4294967295U)
 * # define UINT64_MAX	(__UINT64_C(18446744073709551615))
 */
#define DN_UINT8_MAXLEN (3 + 1)
#define DN_UINT16_MAXLEN (5 + 1)
#define DN_UINT32_MAXLEN (10 + 1)
#define DN_UINT64_MAXLEN (20 + 1)
#define DN_UINTMAX_MAXLEN DN_UINT64_MAXLEN

/*
 * Make data 'd' or pointer 'p', n-byte aligned, where n is a power of 2
 * of 2.
 */
#define DN_ALIGNMENT sizeof(unsigned long) /* platform word */
#define DN_ALIGN(d, n) (((d) + (n - 1)) & ~(n - 1))
#define DN_ALIGN_PTR(p, n) \
  (void *)(((uintptr_t)(p) + ((uintptr_t)n - 1)) & ~((uintptr_t)n - 1))

/*
 * Wrapper to workaround well known, safe, implicit type conversion when
 * invoking system calls.
 */
#define dn_gethostname(_name, _len) gethostname((char *)_name, (size_t)_len)

#define dn_atoi(_line, _n) _dn_atoi((uint8_t *)_line, (size_t)_n)
#define dn_atoui(_line, _n) _dn_atoui((uint8_t *)_line, (size_t)_n)

int dn_set_blocking(int sd);
int dn_set_nonblocking(int sd);
int dn_set_reuseaddr(int sd);
int dn_set_keepalive(int sd, int val);
int dn_set_tcpnodelay(int sd);
int dn_set_linger(int sd, int timeout);
int dn_set_sndbuf(int sd, int size);
int dn_set_rcvbuf(int sd, int size);
int dn_get_soerror(int sd);
int dn_get_sndbuf(int sd);
int dn_get_rcvbuf(int sd);

int _dn_atoi(uint8_t *line, size_t n);
uint32_t _dn_atoui(uint8_t *line, size_t n);
bool dn_valid_port(int n);

/*
 * Memory allocation and free wrappers.
 *
 * These wrappers enables us to loosely detect double free, dangling
 * pointer access and zero-byte alloc.
 */
#define dn_alloc(_s) _dn_alloc((size_t)(_s), __FILE__, __LINE__)

#define dn_zalloc(_s) _dn_zalloc((size_t)(_s), __FILE__, __LINE__)

#define dn_calloc(_n, _s) \
  _dn_calloc((size_t)(_n), (size_t)(_s), __FILE__, __LINE__)

#define dn_realloc(_p, _s) _dn_realloc(_p, (size_t)(_s), __FILE__, __LINE__)

#define dn_free(_p)                   \
  do {                                \
    _dn_free(_p, __FILE__, __LINE__); \
    (_p) = NULL;                      \
  } while (0)

void *_dn_alloc(size_t size, const char *name, int line);
void *_dn_zalloc(size_t size, const char *name, int line);
void *_dn_calloc(size_t nmemb, size_t size, const char *name, int line);
void *_dn_realloc(void *ptr, size_t size, const char *name, int line);
void _dn_free(void *ptr, const char *name, int line);

/*
 * Wrappers to send or receive n byte message on a blocking
 * socket descriptor.
 */
#define dn_sendn(_s, _b, _n) _dn_sendn(_s, _b, (size_t)(_n))

#define dn_recvn(_s, _b, _n) _dn_recvn(_s, _b, (size_t)(_n))

/*
 * Wrappers to read or write data to/from (multiple) buffers
 * to a file or socket descriptor.
 */
#define dn_read(_d, _b, _n) read(_d, _b, (size_t)(_n))

#define dn_readv(_d, _b, _n) readv(_d, _b, (int)(_n))

#define dn_write(_d, _b, _n) write(_d, _b, (size_t)(_n))

#define dn_writev(_d, _b, _n) writev(_d, _b, (int)(_n))

ssize_t _dn_sendn(int sd, const void *vptr, size_t n);
ssize_t _dn_recvn(int sd, void *vptr, size_t n);

/*
 * Wrappers for defining custom assert based on whether macro
 * DN_ASSERT_PANIC or DN_ASSERT_LOG was defined at the moment
 * ASSERT was called.
 */

// http://www.pixelbeat.org/programming/gcc/static_assert.html
#define ASSERT_CONCAT_(a, b) a##b
#define ASSERT_CONCAT(a, b) ASSERT_CONCAT_(a, b)
#ifdef __COUNTER__
#define STATIC_ASSERT(e, m) \
  ;                         \
  enum { ASSERT_CONCAT(static_assert_, __COUNTER__) = 1 / (!!(e)) }
#else
/* This can't be used twice on the same line so ensure if using in headers
 * that the headers are not included twice (by wrapping in #ifndef...#endif)
 * Note it doesn't cause an issue when used on same line of separate modules
 * compiled with gcc -combine -fwhole-program.  */
#define STATIC_ASSERT(e, m) \
  ;                         \
  enum { ASSERT_CONCAT(assert_line_, __LINE__) = 1 / (!!(e)) }
#endif

#ifdef DN_ASSERT_PANIC

#define ASSERT(_x)                           \
  do {                                       \
    if (!(_x)) {                             \
      dn_assert(#_x, __FILE__, __LINE__, 1); \
    }                                        \
  } while (0)

#define ASSERT_LOG(_x, _M, ...)                         \
  do {                                                  \
    if (!(_x)) {                                        \
      log_error("Assertion Failed: "_M, ##__VA_ARGS__); \
      dn_assert(#_x, __FILE__, __LINE__, 1);            \
    }                                                   \
  } while (0)

#define NOT_REACHED() ASSERT(0)

#elif DN_ASSERT_LOG

#define ASSERT(_x)                           \
  do {                                       \
    if (!(_x)) {                             \
      dn_assert(#_x, __FILE__, __LINE__, 0); \
    }                                        \
  } while (0)

#define ASSERT_LOG(_x, _M, ...)                         \
  do {                                                  \
    if (!(_x)) {                                        \
      log_error("ASSERTION FAILED: "_M, ##__VA_ARGS__); \
      dn_assert(#_x, __FILE__, __LINE__, 0);            \
    }                                                   \
  } while (0)

#define NOT_REACHED() ASSERT(0)

#else

#define ASSERT(_x)
#define ASSERT_LOG(_x, _M, ...)

#define NOT_REACHED()

#endif

#ifdef DN_LITTLE_ENDIAN

#define str4cmp(m, c0, c1, c2, c3) \
  (*(uint32_t *)m == ((c3 << 24) | (c2 << 16) | (c1 << 8) | c0))

#define str5cmp(m, c0, c1, c2, c3, c4) \
  (str4cmp(m, c0, c1, c2, c3) && (m[4] == c4))

#define str6cmp(m, c0, c1, c2, c3, c4, c5) \
  (str4cmp(m, c0, c1, c2, c3) &&           \
   (((uint32_t *)m)[1] & 0xffff) == ((c5 << 8) | c4))

#define str7cmp(m, c0, c1, c2, c3, c4, c5, c6) \
  (str6cmp(m, c0, c1, c2, c3, c4, c5) && (m[6] == c6))

#define str8cmp(m, c0, c1, c2, c3, c4, c5, c6, c7) \
  (str4cmp(m, c0, c1, c2, c3) &&                   \
   (((uint32_t *)m)[1] == ((c7 << 24) | (c6 << 16) | (c5 << 8) | c4)))

#define str9cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8) \
  (str8cmp(m, c0, c1, c2, c3, c4, c5, c6, c7) && m[8] == c8)

#define str10cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9) \
  (str8cmp(m, c0, c1, c2, c3, c4, c5, c6, c7) &&            \
   (((uint32_t *)m)[2] & 0xffff) == ((c9 << 8) | c8))

#define str11cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10) \
  (str10cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9) && (m[10] == c10))

#define str12cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11) \
  (str8cmp(m, c0, c1, c2, c3, c4, c5, c6, c7) &&                      \
   (((uint32_t *)m)[2] == ((c11 << 24) | (c10 << 16) | (c9 << 8) | c8)))

#else

#define str4cmp(m, c0, c1, c2, c3) \
  (m[0] == c0 && m[1] == c1 && m[2] == c2 && m[3] == c3)

#define str5cmp(m, c0, c1, c2, c3, c4) \
  (str4cmp(m, c0, c1, c2, c3) && (m[4] == c4))

#define str6cmp(m, c0, c1, c2, c3, c4, c5) \
  (str5cmp(m, c0, c1, c2, c3, c4) && m[5] == c5)

#define str7cmp(m, c0, c1, c2, c3, c4, c5, c6) \
  (str6cmp(m, c0, c1, c2, c3, c4, c5) && m[6] == c6)

#define str8cmp(m, c0, c1, c2, c3, c4, c5, c6, c7) \
  (str7cmp(m, c0, c1, c2, c3, c4, c5, c6) && m[7] == c7)

#define str9cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8) \
  (str8cmp(m, c0, c1, c2, c3, c4, c5, c6, c7) && m[8] == c8)

#define str10cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9) \
  (str9cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8) && m[9] == c9)

#define str11cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10) \
  (str10cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9) && m[10] == c10)

#define str12cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11) \
  (str11cmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10) && m[11] == c11)

#endif

#define str3icmp(m, c0, c1, c2)           \
  ((m[0] == c0 || m[0] == (c0 ^ 0x20)) && \
   (m[1] == c1 || m[1] == (c1 ^ 0x20)) && (m[2] == c2 || m[2] == (c2 ^ 0x20)))

#define str4icmp(m, c0, c1, c2, c3) \
  (str3icmp(m, c0, c1, c2) && (m[3] == c3 || m[3] == (c3 ^ 0x20)))

#define str5icmp(m, c0, c1, c2, c3, c4) \
  (str4icmp(m, c0, c1, c2, c3) && (m[4] == c4 || m[4] == (c4 ^ 0x20)))

#define str6icmp(m, c0, c1, c2, c3, c4, c5) \
  (str5icmp(m, c0, c1, c2, c3, c4) && (m[5] == c5 || m[5] == (c5 ^ 0x20)))

#define str7icmp(m, c0, c1, c2, c3, c4, c5, c6) \
  (str6icmp(m, c0, c1, c2, c3, c4, c5) && (m[6] == c6 || m[6] == (c6 ^ 0x20)))

#define str8icmp(m, c0, c1, c2, c3, c4, c5, c6, c7) \
  (str7icmp(m, c0, c1, c2, c3, c4, c5, c6) &&       \
   (m[7] == c7 || m[7] == (c7 ^ 0x20)))

#define str9icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8) \
  (str8icmp(m, c0, c1, c2, c3, c4, c5, c6, c7) &&       \
   (m[8] == c8 || m[8] == (c8 ^ 0x20)))

#define str10icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9) \
  (str9icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8) &&        \
   (m[9] == c9 || m[9] == (c9 ^ 0x20)))

#define str11icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10) \
  (str10icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9) &&        \
   (m[10] == c10 || m[10] == (c10 ^ 0x20)))

#define str12icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11) \
  (str11icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10) &&        \
   (m[11] == c11 || m[11] == (c11 ^ 0x20)))

#define str13icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12) \
  (str12icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11) &&        \
   (m[12] == c12 || m[12] == (c12 ^ 0x20)))

#define str14icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, \
                  c13)                                                      \
  (str13icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12) &&   \
   (m[13] == c13 || m[13] == (c13 ^ 0x20)))

#define str15icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12,    \
                  c13, c14)                                                    \
  (str14icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13) && \
   (m[14] == c14 || m[14] == (c14 ^ 0x20)))

#define str16icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, \
                  c13, c14, c15)                                            \
  (str15icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, \
             c14) &&                                                        \
   (m[15] == c15 || m[15] == (c15 ^ 0x20)))

#define str17icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, \
                  c13, c14, c15, c16)                                       \
  (str16icmp(m, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, \
             c14, c15) &&                                                   \
   (m[16] == c16 || m[16] == (c16 ^ 0x20)))

void dn_assert(const char *cond, const char *file, int line, int panic);
void dn_stacktrace(int skip_count);

int _scnprintf(char *buf, size_t size, const char *fmt, ...);
int _vscnprintf(char *buf, size_t size, const char *fmt, va_list args);
usec_t dn_usec_now(void);
msec_t dn_msec_now(void);

/*
 * Address resolution for internet (ipv4 and ipv6) and unix domain
 * socket address.
 */

struct sockinfo {
  int family;        /* socket address family */
  socklen_t addrlen; /* socket address length */
  union {
    struct sockaddr_in in;   /* ipv4 socket address */
    struct sockaddr_in6 in6; /* ipv6 socket address */
    struct sockaddr_un un;   /* unix domain address */
  } addr;
};

int dn_resolve(struct string *name, int port, struct sockinfo *si);
char *dn_unresolve_addr(struct sockaddr *addr, socklen_t addrlen);
char *dn_unresolve_peer_desc(int sd);
char *dn_unresolve_desc(int sd);

unsigned int dict_string_hash(const void *key);
int dict_string_key_compare(void *privdata, const void *key1, const void *key2);
void dict_string_destructor(void *privdata, void *val);

/*
 * Counts the total number of digits in 'arg'.
 */
int count_digits(uint64_t arg);

/*
 * Returns the current timestamp in milliseconds.
 */
uint64_t current_timestamp_in_millis(void);

#endif
