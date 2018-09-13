
#ifndef _DYN_SEEDS_PROVIDER_H_
#define _DYN_SEEDS_PROVIDER_H_

#define SEEDS_CHECK_INTERVAL (30 * 1000) /* in msec */

// Forward declarations
struct context;
struct mbuf;

uint8_t florida_get_seeds(struct context *ctx, struct mbuf *seeds_buf);
uint8_t dns_get_seeds(struct context *ctx, struct mbuf *seeds_buf);

#endif /* DYN_SEEDS_PROVIDER_H_ */
