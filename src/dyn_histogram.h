/*
 * dyn_histogram.h
 *
 *  Created on: Feb 6, 2015
 *      Author: mdo
 */

#ifndef DYN_HISTOGRAM_H_
#define DYN_HISTOGRAM_H_


#define BUCKET_SIZE 94



rstatus_t histo_init();
void histo_add(uint64_t val);
uint64_t histo_get_bucket(int bucket);
void histo_get_buckets(uint64_t* my_buckets);
uint64_t histo_percentile(double percentile);
uint64_t histo_mean();
uint64_t histo_max();
void histo_compute_latencies(uint64_t* mean, uint64_t* latency_95th,
		uint64_t* latency_99th, uint64_t* latency_999th, uint64_t* latency_max);

#endif /* DYN_HISTOGRAM_H_ */
