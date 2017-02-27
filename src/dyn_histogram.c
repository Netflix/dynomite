/*
 * dyn_histogram.c
 *
 *  Created on: Feb 6, 2015
 *      Author: mdo
 */

#include <math.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

#include "dyn_core.h"
#include "dyn_conf.h"
#include "dyn_histogram.h"



/* a port from this java code:
 * https://github.com/apache/cassandra/blob/cassandra-1.2/src/java/org/apache/cassandra/utils/EstimatedHistogram.java
 *
 * Will try to use https://github.com/HdrHistogram/HdrHistogram_c  later
 */



static uint64_t bucket_offsets[BUCKET_SIZE];


rstatus_t histo_init(volatile struct histogram *histo)
{
	if (histo == NULL) {
		return DN_ERROR;
	}

	uint64_t *buckets = histo->buckets;
	uint64_t last = 1;
	bucket_offsets[0] = last;
	int i;
	for(i = 1; i<BUCKET_SIZE; i++)
	{
		uint64_t next = (uint64_t)floor((double)last * 1.2);
		if (next == last)
			next++;

		bucket_offsets[i] = next;
		last = next;
	}

	for(i = 0; i<BUCKET_SIZE; i++) {
		buckets[i] = 0;
	}

	histo->mean = 0;
	histo->val_95th = 0;
	histo->val_999th = 0;
	histo->val_99th = 0;
	histo->val_max = 0;

	return DN_OK;
}


rstatus_t histo_reset(volatile struct histogram *histo)
{
	if (histo == NULL) {
		return DN_ERROR;
	}

	uint64_t *buckets = histo->buckets;
	int i;
	for(i = 0; i<BUCKET_SIZE; i++) {
		buckets[i] = 0;
	}

	histo->mean = 0;
	histo->val_95th = 0;
	histo->val_999th = 0;
	histo->val_99th = 0;
	histo->val_max = 0;

	return DN_OK;
}


static uint64_t count(struct histogram *histo)
{
	if (histo == NULL) {
		return 0;
	}

	uint64_t *buckets = histo->buckets;
	uint64_t sum = 0L;
	int i;
	for (i = 0; i < BUCKET_SIZE; i++)
		sum += buckets[i];
	return sum;
}

void histo_add(volatile struct histogram *histo, uint64_t val)
{
	if (histo == NULL) {
		return;
	}

	uint64_t *buckets = histo->buckets;
	int left_index, right_index, middle_index, index = 0;

	left_index = 0;
	right_index = BUCKET_SIZE-1;


	while (left_index < right_index ) {
		middle_index = left_index + (right_index - left_index) / 2;

		if (val == bucket_offsets[middle_index]) {
			index = middle_index;
			break;
		} else if (val < bucket_offsets[middle_index]) {
			right_index = middle_index;
		} else {
			left_index = middle_index;
		}

		if (left_index == right_index - 1) {
			index = left_index;
			break;
		}
	}

	if (left_index == right_index - 1)
		index = left_index;

	buckets[index]++;

	//store max value
	histo->val_max = (histo->val_max > val)? histo->val_max : val;
}


/*uint64_t histo_get_bucket(struct histogram *histo, int bucket)
{
	if (histo == NULL) {
		return -1;
	}

	uint64_t *buckets = histo->buckets;
	if (bucket < BUCKET_SIZE)
		return buckets[bucket];

	return 0;
}


void histo_get_buckets(struct histogram *histo, uint64_t* my_buckets)
{
	if (histo == NULL) {
		return;
	}

	uint64_t *buckets = histo->buckets;
	int i;
	for(i=0; i<BUCKET_SIZE; i++) {
		my_buckets[i] = buckets[i];
	}

}


uint64_t histo_percentile(struct histogram *histo, double percentile)
{
	if (histo == NULL) {
		return -1;
	}

	uint64_t *buckets = histo->buckets;

	if (percentile < 0 && percentile > 1.0) {
		return 0;
	}

	int last_bucket = BUCKET_SIZE - 1;
	if (buckets[last_bucket] > 0) {
		log_error("histogram overflowed!");
		return -1;
	}

	uint64_t pcount = floor(count(histo) * percentile);
	if (pcount == 0)
		return 0;

	uint64_t elements = 0;
	int i;
	for (i = 0; i < last_bucket; i++)
	{
		elements += buckets[i];
		if (elements >= pcount)
			return bucket_offsets[i];
	}

	return 0;
}


uint64_t histo_mean(struct histogram *histo)
{
	if (histo == NULL) {
		return -1;
	}

	uint64_t *buckets = histo->buckets;

	int last_bucket = BUCKET_SIZE - 1;
	if (buckets[last_bucket] > 0) {
		log_error("histogram overflowed!");
		return -1;
	}

	uint64_t elements = 0;
	uint64_t sum = 0;
	int i;
	for (i = 0; i < last_bucket; i++)
	{
		elements += buckets[i];
		sum += buckets[i] * bucket_offsets[i];
	}

	return  ceil((double) sum / elements);
}


uint64_t histo_max(struct histogram *histo)
{
	if (histo == NULL) {
		return -1;
	}

	return histo->val_max;
}
*/

void histo_compute(volatile struct histogram *histo)
{
	if (histo == NULL) {
		return;
	}

	uint64_t *buckets = histo->buckets;

	int last_bucket = BUCKET_SIZE - 1;
	if (buckets[last_bucket] > 0) {
		log_error("histogram overflowed!");
		return;
	}

	uint64_t p95_count = (uint64_t)floor((double)count(histo) * 0.95);
	uint64_t p99_count = (uint64_t)floor((double)count(histo) * 0.99);
	uint64_t p999_count = (uint64_t)floor((double)count(histo) * 0.999);

	uint64_t val_95th = 0;
	uint64_t val_99th = 0;
	uint64_t val_999th = 0;


	uint64_t elements = 0;
	uint64_t sum = 0;
	int i;
	for (i = 0; i < last_bucket; i++)
	{
		elements += buckets[i];
		if (elements >= p95_count && val_95th == 0)
			val_95th = bucket_offsets[i];

		if (elements >= p99_count && val_99th == 0)
			val_99th = bucket_offsets[i];

		if (elements >= p999_count && val_999th == 0)
			val_999th = bucket_offsets[i];

		sum += buckets[i] * bucket_offsets[i];

	}

	if (elements != 0)
	   histo->mean = (uint64_t)ceil((double) sum / (double)elements);

	histo->val_95th = val_95th;
	histo->val_99th = val_99th;
	histo->val_999th = val_999th;
}
