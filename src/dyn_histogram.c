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
static uint64_t buckets[BUCKET_SIZE];
static uint64_t max;


rstatus_t histo_init()
{
	uint64_t last = 1;
	bucket_offsets[0] = last;
	int i;
	for(i = 1; i<BUCKET_SIZE; i++)
	{
		uint64_t next = floor(last * 1.2);
		if (next == last)
			next++;

		bucket_offsets[i] = next;
		last = next;
	}

	for(i = 0; i<BUCKET_SIZE; i++) {
		buckets[i] = 0;
	}

	max = 0;

	return DN_OK;
}


static uint64_t count()
{
	uint64_t sum = 0L;
	int i;
	for (i = 0; i < BUCKET_SIZE; i++)
		sum += buckets[i];
	return sum;
}


int histo_bucket_size()
{
	return BUCKET_SIZE;
}


uint64_t* histo_bucket_offsets()
{
	return bucket_offsets;
}


void histo_add(uint64_t val)
{
	int begin_index, end_index, left_index, right_index, middle_index, index;

	begin_index = left_index = 0;
	end_index = right_index = BUCKET_SIZE-1;


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
	max = (max > val)? max : val;
}


uint64_t histo_get_bucket(int bucket)
{
	if (bucket < BUCKET_SIZE)
		return buckets[bucket];

	return 0;
}


void histo_get_buckets(uint64_t* my_buckets)
{
	int i;
	for(i=0; i<BUCKET_SIZE; i++) {
		my_buckets[i] = buckets[i];
	}

}


uint64_t histo_percentile(double percentile)
{
	if (percentile < 0 && percentile > 1.0) {
		return 0;
	}

	int last_bucket = BUCKET_SIZE - 1;
	if (buckets[last_bucket] > 0) {
		log_error("histogram overflowed!");
		return -1;
	}

	uint64_t pcount = floor(count() * percentile);
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


uint64_t histo_mean()
{
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


uint64_t histo_max()
{
	return max;
}


void histo_compute_latencies(uint64_t* mean, uint64_t* latency_95th,
		uint64_t* latency_99th, uint64_t* latency_999th, uint64_t* latency_max)
{
	if (mean == NULL || latency_95th == NULL || latency_99th == NULL || latency_999th == NULL) {
		return;
	}


	int last_bucket = BUCKET_SIZE - 1;
	if (buckets[last_bucket] > 0) {
		log_error("histogram overflowed!");
		return;
	}

	uint64_t p95_count = floor(count() * 0.95);
	uint64_t p99_count = floor(count() * 0.99);
	uint64_t p999_count = floor(count() * 0.999);

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
	   *mean = ceil((double) sum / elements);

	*latency_95th = val_95th;
	*latency_99th = val_99th;
	*latency_999th = val_999th;
	*latency_max = max;
}


void print_buckets() {
	int i;
	for(i = 0; i<BUCKET_SIZE; i++) {
		loga(" val: %lu -  offset: %lu\n", buckets[i], bucket_offsets[i]);
	}

	printf("\n");
}

void print_bucketoffsets() {
	int i;
	for(i = 0; i<BUCKET_SIZE; i++) {
		loga(" val %lu\n", bucket_offsets[i]);
	}

	loga("\n");

}
