/*
 * dyn_histogram.h
 *
 *  Created on: Feb 6, 2015
 *      Author: mdo
 */

#ifndef DYN_HISTOGRAM_H_
#define DYN_HISTOGRAM_H_


#define BUCKET_SIZE 94


struct histogram {
	uint64_t buckets[BUCKET_SIZE];
	uint64_t mean;
	uint64_t val_95th;
	uint64_t val_99th;
	uint64_t val_999th;
	uint64_t val_max;
};


rstatus_t histo_init(struct histogram *histo);
void histo_add(struct histogram *histo, uint64_t val);
uint64_t histo_get_bucket(struct histogram *histo, int bucket);
void histo_get_buckets(struct histogram *histo, uint64_t* my_buckets);
uint64_t histo_percentile(struct histogram *histo, double percentile);
uint64_t histo_mean(struct histogram *histo);
uint64_t histo_max(struct histogram *histo);
void histo_compute(struct histogram *histo);


#endif /* DYN_HISTOGRAM_H_ */
