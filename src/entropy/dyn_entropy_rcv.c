/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed
 *storages. Copyright (C) 2015 Netflix, Inc.
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

#include <fcntl.h>  // for open
#include <math.h>   // to do ceil for number of chunks
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>  //for close

#include <openssl/conf.h>
#include <openssl/err.h>
#include <openssl/evp.h>

#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "dyn_core.h"

/*
 * Function:  entropy_redis_connector
 * --------------------
 *
 *  returns: rstatus_t for the status of opening of the redis connection.
 */

static int entropy_redis_connector() {
  loga("trying to connect to Redis...");

  struct sockaddr_in serv_addr;
  int sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0) {
    log_error("open socket to Redis failed");
    return -1;
  }
  bzero((char *)&serv_addr, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = htonl(
      INADDR_LOOPBACK); /* set destination IP number - localhost, 127.0.0.1*/
  serv_addr.sin_port = htons(22122);
  if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
    log_error("connecting to Redis failed");
    return -1;
  }

  loga("redis-server connection established: %d", sockfd);
  return sockfd;
}

/*
 * Function:  entropy_rcv_start
 * --------------------
 *
 * Receives the keys from the entropy engine
 * and pushes them to Redis.
 */
rstatus_t entropy_rcv_start(int peer_socket, int header_size, int buffer_size,
                            int cipher_size) {
  int redis_socket = 0;
  char aof[buffer_size];
  char buff[buffer_size];
  unsigned char ciphertext[cipher_size];
  int32_t keyValueLength;
  int32_t tempInt;
  int i = 0;
  int numberOfKeys;
  int redis_written_bytes = 0;

  /* Check the encryption flag and initialize the crypto */
  if (DECRYPT_FLAG == 1) {
    entropy_crypto_init();
  } else {
    loga("Encryption is disabled for entropy receiver");
  }

  /* Processing header for number of Keys */
  if (DECRYPT_FLAG == 1) {
    int bytesRead = read(peer_socket, ciphertext, cipher_size);
    if (bytesRead < 1) {
      log_error("Error on receiving number of keys --> %s", strerror(errno));
      goto error;
    }
    loga("Bytes read %d", bytesRead);
    if (entropy_decrypt(ciphertext, buffer_size, buff) < 0) {
      log_error("Error decrypting the AOF file size");
      goto error;
    }
    numberOfKeys = ntohl(buff);

  } else {
    if (read(peer_socket, &tempInt, sizeof(int32_t)) < 1) {
      log_error("Error on receiving number of keys --> %s", strerror(errno));
      goto error;
    }
    numberOfKeys = ntohl(tempInt);
  }
  if (numberOfKeys < 0) {
    log_error("receive header not processed properly");
    goto error;
  } else if (numberOfKeys == 0) {
    log_error("no keys sent");
    goto error;
  }
  loga("Expected number of keys: %d", numberOfKeys);

  /* Connect to redis-server */
  redis_socket = entropy_redis_connector();
  if (redis_socket == -1) {
    goto error;
  }

  /* Iterating around the keys */
  for (i = 0; i < numberOfKeys; i++) {
    /*
     * if the encrypt flag is set then, we need to decrypt the aof size
     * and then decrypt the key/OldValue/newValue in Redis serialized format.
     */
    if (DECRYPT_FLAG == 1) {
      if (read(peer_socket, ciphertext, cipher_size) < 1) {
        log_error("Error on receiving aof size --> %s", strerror(errno));
        goto error;
      }
      if (entropy_decrypt(ciphertext, buffer_size, buff) < 0) {
        log_error("Error decrypting the buffer for AOF file size");
        goto error;
      }
      keyValueLength = ntohl(buff);
      log_info("AOF Length: %d", keyValueLength);
      memset(&aof[0], 0, sizeof(aof));
      if (read(peer_socket, ciphertext, cipher_size) < 1) {
        log_error("Error on receiving aof size --> %s", strerror(errno));
        goto error;
      }
      if (entropy_decrypt(ciphertext, buffer_size, aof) <
          0)  // TODO: I am not sure the buffer_size is correct here.
      {
        log_error("Error decrypting the buffer for key/oldValue/newValue");
        goto error;
      }
    } else {
      /* Step 1: Read the key/Value size */
      if (read(peer_socket, &keyValueLength, sizeof(int32_t)) < 1) {
        log_error("Error on receiving aof size --> %s", strerror(errno));
        goto error;
      }
      keyValueLength = ntohl(keyValueLength);
      log_info("AOF Length: %d", keyValueLength);
      memset(&aof[0], 0, sizeof(aof));

      /* Step 2: Read the key/Value using the keyValueLength */
      if (read(peer_socket, &aof, keyValueLength) < 1) {
        log_error("Error on receiving aof file --> %s", strerror(errno));
        goto error;
      }
    }
    loga("Key: %d/%d - Redis serialized form: \n%s", i + 1, numberOfKeys, aof);
    redis_written_bytes = write(redis_socket, &aof, keyValueLength);
    if (redis_written_bytes < 1) {
      log_error("Error on writing to Redis, bytes: %d --> %s",
                redis_written_bytes, strerror(errno));
      goto error;
    }
    loga("Bytes written to Redis %d", redis_written_bytes);
  }

  if (redis_socket > -1) close(redis_socket);

  return DN_OK;

error:

  if (redis_socket > -1) {
    close(redis_socket);
    log_error("entropy rcv closing redis socket because of error.");
  }

  return DN_ERROR;
}
