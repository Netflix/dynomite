/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2015 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *_stats_pool_set_ts
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h> // for open
#include <unistd.h> //for close
#include <math.h> // to do ceil for number of chunks

#include <openssl/conf.h>
#include <openssl/evp.h>
#include <openssl/err.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/sendfile.h>
#include <netinet/in.h>

#include "dyn_core.h"

#define LOG_CHUNK_LEVEL			1000 // every how many chunks to log
#define THROUGHPUT_THROTTLE		10000000
#define AOF_TO_SEND		"/mnt/data/nfredis/appendonly.aof"	/* add in .yml */


/*
 * Function:  entropy_redis_compact_aof
 * --------------------
 *
 * Performs background Redis rewrite of aof.
 * It tries to rewrite the aof twice before sending to Spark.
 * If the second time fails, the Socket to spark is closed.
 */

static rstatus_t
entropy_redis_compact_aof(int buffer_size){
	char 			command[buffer_size];
    int 			sys_ret = 0;

	memset(&command[0], 0, sizeof(command));
    sprintf(command, "redis-cli -p 22122 bgrewriteaof");
    sys_ret = system(command);
    if( sys_ret < 0 ){
	    log_error("Error on system call --> %s", strerror(errno));
	    loga("Thread sleeping 10 seconds and retrying");
	    sleep(10);
	    sys_ret = system(command);
	    if( sys_ret < 0 ){
		    log_error("Error on bgrewriteaof for seconds time --> %s", strerror(errno));
			return DN_ERROR;
	    }
    }
    else if( sys_ret > 0 ){
    	log_error("Cannot connect to Redis on port 22122: %d", sys_ret);
    	return DN_ERROR;
    }
    loga("Redis BGREWRITEAOF completed");
    return DN_OK;
}

/*
 * Function:  header_send
 * --------------------
 *
 * Sending summary information in a header;
 * Header Format: file size | encryption | data store
 *
 */
static rstatus_t
header_send(struct stat file_stat, int peer_socket, int header_size)
{
    char			header_buff[header_size];
    ssize_t         transmit_len;

    memset(&header_buff[0], 0, sizeof(header_buff));
    header_buff[0] = (int)((((int)file_stat.st_size) >> 24) & 0xFF);
    header_buff[1] = (int)((((int)file_stat.st_size) >> 16) & 0xFF);
    header_buff[2] = (int)((((int)file_stat.st_size) >> 8) & 0XFF);
    header_buff[3] = (int)((((int)file_stat.st_size) & 0XFF));

    // TODO: encrypt flag does not have to be int but a single byte.
    header_buff[4] = (int)((ENCRYPT_FLAG >> 24) & 0xFF);
    header_buff[5] = (int)((ENCRYPT_FLAG >> 16) & 0xFF);
    header_buff[6] = (int)((ENCRYPT_FLAG >> 8) & 0XFF);
    header_buff[7] = (int)((ENCRYPT_FLAG & 0XFF));

    //TODO: we can add data store information as well

  	transmit_len = send(peer_socket, header_buff, sizeof(header_buff), 0);
  	if (transmit_len < 0)
  	{
  	    log_error("Error on sending AOF file size --> %s", strerror(errno));
      	return DN_ERROR;
    }

  	loga("The size of header is %d",sizeof(header_buff)); //TODO: this can be moved to log_info
  	return DN_OK;
}

/*
 * Function:  entropy_snd_stats
 * --------------------
 *
 * Logging statistics about the transfer;
 *
 */
static void
entropy_snd_stats(int current_chunk, time_t elapsed_time, double chunk_thr, double byte_thr){

	if(elapsed_time > 0 && current_chunk > 0){
        loga("Transferring chunk %d (%.2f chunks/sec"
        		 "  -- %.2f MB/sec)", current_chunk, chunk_thr, byte_thr);
	}
}

/*
 * Function:  entropy_snd_start
 * --------------------
 *
 * Processes the AOF and transmits to the entropy engine
 */
rstatus_t
entropy_snd_start(int peer_socket, int header_size, int buffer_size, int cipher_size){

    struct stat     file_stat;
    ssize_t         transmit_len;
    ssize_t			data_trasmitted = 0;
    FILE			*fp = NULL;
    int             fd;
    char            data_buff[buffer_size];
    unsigned char ciphertext[cipher_size];
    int ciphertext_len = 0;
    size_t 			aof_bytes_read;
    int				nchunk;
    int 			i; //iterator for chunks
    size_t 			last_chunk_size;
	double chunk_thr = 0;
	double byte_thr = 0;
	time_t elapsed_time;

    /* compact AOF in Redis before sending to Spark */
    if(entropy_redis_compact_aof(buffer_size) == DN_ERROR){
    	log_error("Redis failed to perform bgrewriteaof");
    	goto error;
    }
    /* short sleep to finish AOF rewriting */
    sleep(1);

    /* create a file pointer for the AOF */
    fp = fopen(AOF_TO_SEND, "r");
    if (fp == NULL)
    {
    	log_error("Error opening Redis AOF file: %s", strerror(errno));
    	goto error;
    }

    /* Get the file descriptor from the file pointer */
    fd = fileno(fp);

    /* Get the file size */
    if (fstat(fd, &file_stat) < 0)
    {
    	 log_error("Error fstat --> %s", strerror(errno));
     	 goto error;
    }

    /* No file AOF found to send */
    if(file_stat.st_size == 0){
    	log_error("Cannot retrieve an AOF file in %s", AOF_TO_SEND);
    	goto error;
    }
    loga("Redis appendonly.aof ready to be sent");


    /* sending header */
    if(header_send(file_stat, peer_socket, header_size)==DN_ERROR){
    	goto error;
    }

	/* Determine the number of chunks
	 * if the size of the file is larger than the Buffer size
	 * then split it, otherwise we need one chunk only.
	 *  */
	if(file_stat.st_size > buffer_size){
		nchunk = (int)(ceil(file_stat.st_size/buffer_size) + 1);
	}
	else{
		nchunk = 1;
	}

    /* Last chunk size is calculated by subtracting from the total file size
     * the size of each chunk excluding the last one.
     */
   	last_chunk_size = (long)(file_stat.st_size - (nchunk-1) * buffer_size);

	loga("HEADER INFO: file size: %d -- buffer size: %d -- cipher size: %d -- encryption: %d ",
			(int)file_stat.st_size, buffer_size, cipher_size, ENCRYPT_FLAG);
	loga("CHUNK INFO: number of chunks: %d -- last chunk size: %ld", nchunk, last_chunk_size);

	time_t stats_start_time = time(NULL);
	struct timeval now;
	gettimeofday(&now, NULL);
	time_t      throttle_start_sec = now.tv_sec;
	suseconds_t throttle_start_usec = now.tv_usec;
	suseconds_t throttle_elapsed_usec;
	suseconds_t throttle_current_rate_usec;

	int stat_chunks_in_window = 0;
	ssize_t stat_bytes_in_window = 0;
	ssize_t throttle_bytes = 0;

    for(i=0; i<nchunk; i++){

        /* clear buffer before using it */
        memset(data_buff, 0, sizeof(data_buff));

        /* Read file data in chunks of buffer_size bytes */
        if(i < nchunk-1){
        	aof_bytes_read = fread (data_buff, sizeof(char), buffer_size, fp);
        }
        else{
        	aof_bytes_read = fread (data_buff, sizeof(char), last_chunk_size, fp);
        }

        /* checking for errors */
    	if (aof_bytes_read < 0){
    		 log_error("Error reading chunk of AOF file --> %s", strerror(errno));
         	 goto error;
    	}

    	/***** THROTTLER ******/

    	/* Capture the current time, the elapsed time, and the bytes */
    	gettimeofday(&now, NULL);
    	throttle_elapsed_usec = (now.tv_sec-throttle_start_sec)*1000000 + now.tv_usec-throttle_start_usec;
    	throttle_bytes += aof_bytes_read;

    	/* Determine the expected throughput on the usec level */
    	throttle_current_rate_usec = (suseconds_t) 1000000 * throttle_bytes/THROUGHPUT_THROTTLE;

    	/* if the rate is higher than the expected, then wait for the corresponding time to throttle it */
    	if (throttle_current_rate_usec  > throttle_elapsed_usec ){
    		usleep(throttle_current_rate_usec - throttle_elapsed_usec);
    		throttle_bytes = 0;
    		throttle_start_sec = now.tv_sec;
    		throttle_start_usec = now.tv_usec;
    	}
    	/******************/

        if(ENCRYPT_FLAG == 1){
        	if (i < nchunk-1){
                ciphertext_len = entropy_encrypt (data_buff, buffer_size, ciphertext);
        	}
        	else{
                ciphertext_len = entropy_encrypt (data_buff, last_chunk_size, ciphertext);
                loga("Size of last chunk: %d", sizeof(data_buff));
        	}
        	if(ciphertext_len < 0){
        		log_error("Error encrypting the AOF chunk --> %s", strerror(errno));
            	 goto error;
        	}
        	transmit_len = send(peer_socket, ciphertext, sizeof(ciphertext), 0);
        }
        else{
        	if(i<nchunk-1){
        		transmit_len = send(peer_socket, data_buff, buffer_size, 0);
        	}
        	else{
        		transmit_len = send(peer_socket, data_buff, last_chunk_size, 0);
        	}
        }

    	if (transmit_len < 0){
    		 log_error("Error sending the AOF chunk --> %s", strerror(errno));
    		 log_error("Data transmitted up to error: %ld and chunks: %d", data_trasmitted, i+1);
         	 goto error;
    	}
    	else if ( transmit_len == 0){
    		 loga("No data in chunk");
    	}
    	else{
    		data_trasmitted +=transmit_len;
    		stat_chunks_in_window++;
    		stat_bytes_in_window +=transmit_len;

			elapsed_time = time(NULL) - stats_start_time;

    		if (elapsed_time > 0 && (i % LOG_CHUNK_LEVEL == 0 || i == nchunk)){
    	        chunk_thr = (double)(stat_chunks_in_window/elapsed_time);
    	        byte_thr = (double)(stat_bytes_in_window/elapsed_time)/1000000; //Divide by 1M for MB
    			entropy_snd_stats(i, elapsed_time, chunk_thr, byte_thr);
    			stat_bytes_in_window = 0;
    			stat_chunks_in_window = 0;
    			stats_start_time = time(NULL);
    		}
    	}
    }

    loga("Chunks transferred: %d ---> AOF transfer completed!", i);
	if(fp!=NULL)
		fclose(fp);

    return DN_OK;

error:

	if(fp!=NULL)
		fclose(fp);

    return DN_ERROR;

}


