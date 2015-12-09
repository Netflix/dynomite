#include <stdio.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <netdb.h>

#include "dyn_seeds_provider.h"
#include "dyn_core.h"
#include "dyn_string.h"

/***************************************************************************
 * Keep polling the local REST service at the address
 * http://127.0.0.1:8080/REST/v1/admin/get_seeds
 * Expect response is a list of peers in the format:
 *   peer_host1:peer_listen_port:rack:dc:peer_token1|peer_host2:peer_listen_port:rack:dc:peer_token2|...
 * For example:
 *   ec2-54-145-17-101.compute-1.amazonaws.com:8101:dyno_pds--useast1e:us-east-1:1383429731|ec2-54-101-51-17.eu-west-1.compute.amazonaws.com:8101:dyno_pds--euwest1c:eu-west-1:1383429731
 ****************************************************************************/

#ifndef FLORIDA_IP
#define FLORIDA_IP "127.0.0.1"
#endif

#ifndef FLORIDA_PORT
#define FLORIDA_PORT 8080
#endif

#ifndef FLORIDA_REQUEST
#define FLORIDA_REQUEST "GET /REST/v1/admin/get_seeds HTTP/1.0\r\nHost: 127.0.0.1\r\nUser-Agent: HTMLGET 1.0\r\n\r\n";
#endif

static char * floridaIp   = NULL;
static char * floridaPort = NULL;
static char * request     = NULL;
static bool isOsVarEval   = true;

static void evalOSVar(void);

static uint32_t create_tcp_socket();
static uint8_t *build_get_query(uint8_t *host, uint8_t *page);

static int64_t last = 0; //storing last time for seeds check
static uint32_t last_seeds_hash = 0;

static void evalOSVar(void){
  if (isOsVarEval==true)	{
  	 request     = (getenv("DYNOMITE_FLORIDA_REQUEST")!=NULL) ? getenv("DYNOMITE_FLORIDA_REQUEST") : FLORIDA_REQUEST;
     floridaPort = (getenv("DYNOMITE_FLORIDA_PORT")!=NULL)    ? getenv("DYNOMITE_FLORIDA_PORT")    : FLORIDA_PORT;
     floridaIp   = (getenv("DYNOMITE_FLORIDA_IP")!=NULL)      ? getenv("DYNOMITE_FLORIDA_IP")      : FLORIDA_IP;	
     isOsVarEval = false;
  }
}

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
florida_get_seeds(struct context * ctx, struct mbuf *seeds_buf) {
	struct sockaddr_in *remote;
	uint32_t sock;
	uint32_t tmpres;
	uint8_t *get;
	uint8_t buf[BUFSIZ + 1];

	evalOSVar();
	log_debug(LOG_VVERB, "Running florida_get_seeds!");

	if (!seeds_check()) {
		return DN_NOOPS;
	}

	sock = create_tcp_socket();
	if (sock == -1) {
		log_debug(LOG_VVERB, "Unable to create a socket");
		return DN_ERROR;
	}

	remote = (struct sockaddr_in *) dn_alloc(sizeof(struct sockaddr_in *));
	remote->sin_family = AF_INET;
	tmpres = inet_pton(AF_INET, floridaIp, (void *)(&(remote->sin_addr.s_addr)));
	remote->sin_port = htons(floridaPort);

	if(connect(sock, (struct sockaddr *)remote, sizeof(struct sockaddr)) < 0) {
		log_debug(LOG_VVERB, "Unable to connect the destination");
		return DN_ERROR;
	}

	uint32_t sent = 0;
	while(sent < dn_strlen(request))
	{
		tmpres = send(sock, request+sent, dn_strlen(request)-sent, 0);
		if(tmpres == -1){
			log_debug(LOG_VVERB, "Unable to send query");
            close(sock);
            dn_free(remote);
			return DN_ERROR;
		}
		sent += tmpres;
	}

	mbuf_rewind(seeds_buf);

	memset(buf, 0, sizeof(buf));
	uint32_t htmlstart = 0;
	uint8_t * htmlcontent;
    uint8_t *ok = NULL;

	//assume that the respsone payload is under BUF_SIZE
	while ((tmpres = recv(sock, buf, BUFSIZ, 0)) > 0) {

        // Look for a OK response  in the first buffer output.
        if (!ok)
		    ok = (uint8_t *) strstr((char *)buf, "200 OK\r\n");
        if (ok == NULL) {
            log_error("Received Error from Florida while getting seeds");
            loga_hexdump(buf, tmpres, "Florida Response with %ld bytes of data", tmpres);
            close(sock);
            dn_free(remote);
            return DN_ERROR;
        }

		if (htmlstart == 0) {
			/* Under certain conditions this will not work.
			 * If the \r\n\r\n part is splitted into two messages
			 * it will fail to detect the beginning of HTML content
			 */
			htmlcontent = (uint8_t *) strstr((char *)buf, "\r\n\r\n");
			if(htmlcontent != NULL) {
				htmlstart = 1;
				htmlcontent += 4;
			}
		} else {
			htmlcontent = buf;
		}

		if(htmlstart) {
			mbuf_copy(seeds_buf, htmlcontent, tmpres - (htmlcontent - buf));
		}

		memset(buf, 0, tmpres);
	}

	if(tmpres < 0) {
		log_debug(LOG_VVERB, "Error receiving data");
	}

	close(sock);
	dn_free(remote);

	uint32_t seeds_hash = hash_seeds(seeds_buf->pos, mbuf_length(seeds_buf));

	if (last_seeds_hash != seeds_hash) {
		last_seeds_hash = seeds_hash;
	} else {
		return DN_NOOPS;
	}

	return DN_OK;
}


uint32_t create_tcp_socket()
{
	uint32_t sock;
	if((sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0){
		log_debug(LOG_VVERB, "Unable to create TCP socket");
		return DN_ERROR;
	}
	return sock;
}
