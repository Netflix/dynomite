#include <stdio.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <netdb.h>

#include "dyn_seeds_provider.h"
#include "dyn_core.h"
#include "dyn_string.h"

#define USERAGENT "HTMLGET 1.0"
#define REQ_HEADER "GET /%s HTTP/1.0\r\nHost: %s\r\nUser-Agent: %s\r\n\r\n"

#define IP "127.0.0.1"
#define PAGE "REST/v1/admin/get_seeds"
#define PORT 8080

static uint32_t create_tcp_socket();
static uint8_t *build_get_query(uint8_t *host, uint8_t *page);


static int64_t last; //storing last time for seeds check
static struct string last_seeds;


static bool seeds_check()
{
	int64_t now = nc_msec_now();

	int delta = (int)(now - last);
	if (delta > SEEDS_CHECK_INTERVAL) {
		last = now;
		return true;
	}

	return false;
}


uint8_t florida_get_seeds(struct context * ctx, struct string *seeds) {
	struct sockaddr_in *remote;
	uint32_t sock;
	uint32_t tmpres;
	uint8_t *get;
	uint8_t buf[BUFSIZ+1];

	log_debug(LOG_VVERB, "Running florida_get_seeds!");

	if (!seeds_check()) {
		return NC_NOOPS;
	}

	sock = create_tcp_socket();
	if (sock == -1) {
		log_debug(LOG_VVERB, "Unable to create a socket");
		return NC_ERROR;
	}

	remote = (struct sockaddr_in *) nc_alloc(sizeof(struct sockaddr_in *));
	remote->sin_family = AF_INET;
	tmpres = inet_pton(AF_INET, IP, (void *)(&(remote->sin_addr.s_addr)));
	remote->sin_port = htons(PORT);

	if(connect(sock, (struct sockaddr *)remote, sizeof(struct sockaddr)) < 0) {
		log_debug(LOG_VVERB, "Unable to connect the destination");
		return NC_ERROR;
	}
	get = build_get_query((uint8_t*) IP, (uint8_t*) PAGE);

	uint32_t sent = 0;
	while(sent < nc_strlen(get))
	{
		tmpres = send(sock, get+sent, nc_strlen(get)-sent, 0);
		if(tmpres == -1){
			log_debug(LOG_VVERB, "Unable to send query");
			return NC_ERROR;
		}
		sent += tmpres;
	}

	memset(buf, 0, sizeof(buf));
	uint32_t htmlstart = 0;
	uint8_t * htmlcontent;

	//assume that the respsone payload is under BUFSIZ
	while((tmpres = recv(sock, buf, BUFSIZ, 0)) > 0) {

		if(htmlstart == 0) {
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
			string_copy(seeds, htmlcontent, tmpres - (htmlcontent - buf));
		}

		memset(buf, 0, tmpres);
	}

	if(tmpres < 0) {
		log_debug(LOG_VVERB, "Error receiving data");
	}

	nc_free(get);
	nc_free(remote);
	close(sock);

	if (last_seeds.data == NULL) {  //first time
		string_init(&last_seeds);
		string_copy(&last_seeds, seeds->data, seeds->len);
	} else {
		if (string_compare(&last_seeds, seeds) == 0) { //if equals, no change
			return NC_NOOPS;
		} else {                                   //else, set last_seeds to the latest value
			string_deinit(&last_seeds);
			string_copy(&last_seeds, seeds->data, seeds->len);
		}
	}

	return NC_OK;
}


uint32_t create_tcp_socket()
{
	uint32_t sock;
	if((sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0){
		log_debug(LOG_VVERB, "Unable to create TCP socket");
		return NC_ERROR;
	}
	return sock;
}


uint8_t *build_get_query(uint8_t *host, uint8_t *page)
{
	uint8_t *query;
	uint8_t *getpage = page;

	if(getpage[0] == '/'){
		getpage = getpage + 1;
		//fprintf(stderr,"Removing leading \"/\", converting %s to %s\n", page, getpage);
	}

	// -5 is to consider the %s %s %s in REQ_HEADER and the ending \0
	query = (uint8_t *) nc_alloc(nc_strlen(host) + nc_strlen(getpage) + nc_strlen(USERAGENT) + nc_strlen(REQ_HEADER) - 5);

	nc_sprintf(query, REQ_HEADER, getpage, host, USERAGENT);
	return query;
}

