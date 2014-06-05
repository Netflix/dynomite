#ifndef DYN_SEEDS_PROVIDER_H_
#define DYN_SEEDS_PROVIDER_H_

#include "dyn_core.h"


#define SEEDS_CHECK_INTERVAL  (30 * 1000) /* in msec */


uint8_t florida_get_seeds(struct context * ctx, struct string *seeds);


#endif /* DYN_SEEDS_PROVIDER_H_ */
