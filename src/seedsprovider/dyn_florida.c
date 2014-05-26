#include <stdio.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <stdlib.h>
#include <netdb.h>
#include <string.h>

#include "dyn_florida.h"

static uint32_t create_tcp_socket();
static uint8_t *build_get_query(uint8_t *host, uint8_t *page);


uint8_t get_seeds() {
	struct sockaddr_in *remote;
	uint32_t sock;
	uint32_t tmpres;
	uint8_t *get;
	uint8_t buf[BUFSIZ+1];

	//printf("BUFSIZ %d", BUFSIZ);
	sock = create_tcp_socket();
	//ip = get_ip(host);
	remote = (struct sockaddr_in *) malloc(sizeof(struct sockaddr_in *));
	remote->sin_family = AF_INET;
	tmpres = inet_pton(AF_INET, IP, (void *)(&(remote->sin_addr.s_addr)));
	remote->sin_port = htons(PORT);

	if(connect(sock, (struct sockaddr *)remote, sizeof(struct sockaddr)) < 0) {
		perror("Could not connect");
		exit(1);
	}
	get = build_get_query(IP, PAGE);
	//fprintf(stderr, "Query is:\n<<START>>\n%s<<END>>\n", get);

	//Send the query to the server
	uint32_t sent = 0;
	while(sent < strlen(get))
	{
		tmpres = send(sock, get+sent, strlen(get)-sent, 0);
		if(tmpres == -1){
			perror("Can't send query");
			exit(1);
		}
		sent += tmpres;
	}

	//now it is time to receive the page
	memset(buf, 0, sizeof(buf));
	uint32_t htmlstart = 0;
	uint8_t * htmlcontent;
	while((tmpres = recv(sock, buf, BUFSIZ, 0)) > 0) {
		if(htmlstart == 0) {
			/* Under certain conditions this will not work.
			 * If the \r\n\r\n part is splitted into two messages
			 * it will fail to detect the beginning of HTML content
			 */
			htmlcontent = strstr(buf, "\r\n\r\n");
			if(htmlcontent != NULL) {
				htmlstart = 1;
				htmlcontent += 4;
			}
		}else {
			htmlcontent = buf;
		}

		if(htmlstart){
			fprintf(stdout, htmlcontent);
		}

		memset(buf, 0, tmpres);
	}

	if(tmpres < 0) {
		perror("Error receiving data");
	}

	free(get);
	free(remote);
	//free(ip);
	close(sock);
	return 0;
}




uint32_t create_tcp_socket()
{
	uint32_t sock;
	if((sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0){
		perror("Can't create TCP socket");
		exit(1);
	}
	return sock;
}


uint8_t *build_get_query(uint8_t *host, uint8_t *page)
{
	uint8_t *query;
	uint8_t *getpage = page;
	uint8_t *tpl = "GET /%s HTTP/1.0\r\nHost: %s\r\nUser-Agent: %s\r\n\r\n";
	if(getpage[0] == '/'){
		getpage = getpage + 1;
		//fprintf(stderr,"Removing leading \"/\", converting %s to %s\n", page, getpage);
	}
	// -5 is to consider the %s %s %s in tpl and the ending \0
	query = (uint8_t *)malloc(strlen(host)+strlen(getpage)+strlen(USERAGENT)+strlen(tpl)-5);
	sprintf(query, tpl, getpage, host, USERAGENT);
	return query;
}

