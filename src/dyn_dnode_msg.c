/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 

#include <ctype.h>

#include "dyn_core.h"
#include "dyn_dnode_msg.h"
#include "dyn_server.h"
#include "proto/dyn_proto.h"


static uint64_t dmsg_id;          /* message id counter */
static uint32_t nfree_dmsgq;      /* # free msg q */
static struct dmsg_tqh free_dmsgq; /* free msg q */

static uint32_t MAGIC_NUMBER = 2014;

static const struct string MAGIC_STR = string("2014 ");
static const struct string CRLF_STR = string(CRLF);

static rstatus_t dmsg_to_gossip(struct ring_msg *rmsg);

enum {
        DYN_START = 0,
        DYN_MAGIC_NUMBER = 1000,
        DYN_SPACES_BEFORE_MSG_ID,
        DYN_MSG_ID,
        DYN_SPACES_BEFORE_TYPE_ID,
        DYN_TYPE_ID,
        DYN_SPACES_BEFORE_BIT_FIELD,
        DYN_BIT_FIELD,
        DYN_SPACES_BEFORE_VERSION,
        DYN_VERSION,
        DYN_SPACES_BEFORE_STAR,
        DYN_STAR,
        DYN_DATA_LEN,
        DYN_SPACE_BEFORE_DATA,
        DYN_DATA,
        DYN_SPACES_BEFORE_PAYLOAD_LEN,
        DYN_PAYLOAD_LEN,
        DYN_CRLF_BEFORE_DONE,
        DYN_DONE
} state;


static bool 
dyn_parse_core(struct msg *r)
{
	struct dmsg *dmsg;
	struct mbuf *b;
	uint8_t *p;
	uint8_t ch;
	uint64_t num = 0;

	state = r->dyn_state;
	b = STAILQ_LAST(&r->mhdr, mbuf, next);

	dmsg = r->dmsg;
	if (dmsg == NULL) {
		r->dmsg = dmsg_get();
		dmsg = r->dmsg;
		if (dmsg == NULL) {//should track this as a dropped message
			goto error; //should count as OOM error
		}
	}

	//log_hexdump(LOG_VERB, b->pos, mbuf_length(b), "dyn parser: parsed req %"PRIu64" res %d type %d", r->id, r->result, r->type, r->dyn_state);

	for (p = r->pos; p < b->last; p++) {
		ch = *p;
		switch (state) {
		case DYN_START:
			//log_debug(LOG_DEBUG, "DYN_START");
			if (ch == ' ') {
				break;
			} else if (isdigit(ch)) {
				num = ch - '0';
				state = DYN_MAGIC_NUMBER;
			} else {
				goto skip;
			}

			break;

		case DYN_MAGIC_NUMBER:
			//log_debug(LOG_DEBUG, "DYN_MAGIC_NUMBER");
			//log_debug(LOG_DEBUG, "num = %d", num);
			if (isdigit(ch))  {
				num = num*10 + (ch - '0');
			} else {
				if (num == MAGIC_NUMBER) {
					state = DYN_SPACES_BEFORE_MSG_ID;
				} else {
					goto error;
				}
			}

			break;

		case DYN_SPACES_BEFORE_MSG_ID:
			//log_debug(LOG_DEBUG, "DYN_SPACES_BEFORE_MSG_ID");
			if (ch == ' ') {
				break;
			} else if (isdigit(ch)) {
				num = ch - '0';
				state = DYN_MSG_ID;
			}  else {
				goto error;
			}

			break;

		case DYN_MSG_ID:
			//log_debug(LOG_DEBUG, "DYN_MSG_ID");
			//log_debug(LOG_DEBUG, "num = %d", num);
			if (isdigit(ch))  {
				num = num*10 + (ch - '0');
			} else if (ch != ' ') {
				goto error;
			} else {

				//if (num >= 0) {
				//log_debug(LOG_DEBUG, "MSG ID : %d", num);
				dmsg->id = num;
				state = DYN_SPACES_BEFORE_TYPE_ID;
				//} else {
					//   goto error;
				//}
			}
			break;

		case DYN_SPACES_BEFORE_TYPE_ID:
			//log_debug(LOG_DEBUG, "DYN_SPACES_BEFORE_TYPE_ID");
			if (ch == ' ') {
				break;
			} else if (isdigit(ch)) {
				num = ch - '0';
				state = DYN_TYPE_ID;
			}  else {
				goto error;
			}

			break;

		case DYN_TYPE_ID:
			log_debug(LOG_DEBUG, "DYN_TYPE_ID");
			log_debug(LOG_DEBUG, "num = %d", num);
			if (isdigit(ch))  {
				num = num*10 + (ch - '0');
			} else {
				if (num > 0)  {
					log_debug(LOG_DEBUG, "Type Id: %d", num);
					dmsg->type = num;
					//state = DYN_SPACES_BEFORE_VERSION;
					state = DYN_SPACES_BEFORE_BIT_FIELD;
				} else {
					goto error;
				}
			}

			break;

		case DYN_SPACES_BEFORE_BIT_FIELD:
			if (ch == ' ') {
				break;
			} else if (isdigit(ch)) {
				num = ch - '0';
				state = DYN_BIT_FIELD;
			} else {
				goto error;
			}
			break;

		case DYN_BIT_FIELD:
			log_debug(LOG_DEBUG, "DYN_BIT_FIELD");
			log_debug(LOG_DEBUG, "num = %d", num);
			if (isdigit(ch))  {
				num = num*10 + (ch - '0');
			} else {
				if (ch == ' ')  {
					log_debug(LOG_DEBUG, "DYN_BIT_FIELD : %d", num);
					dmsg->bit_field = num & 0xF;
					state = DYN_SPACES_BEFORE_VERSION;
				} else {
					goto error;
				}
			}

			log_debug(LOG_DEBUG, "Post DYN_BIT_FIELD");
			log_debug(LOG_DEBUG, "num = %d", num);

			break;

		case DYN_SPACES_BEFORE_VERSION:
			//log_debug(LOG_DEBUG, "DYN_SPACES_BEFORE_VERSION");
			if (ch == ' ') {
				break;
			} else if (isdigit(ch)) {
				num = ch - '0';
				state = DYN_VERSION;
			}  else {
				goto error;
			}

			break;

		case DYN_VERSION:
			//log_debug(LOG_DEBUG, "DYN_VERSION");
			//log_debug(LOG_DEBUG, "num = %d", num);
			if (isdigit(ch))  {
				num = num*10 + (ch - '0');
			} else {
				if (ch == ' ')  {
					//log_debug(LOG_DEBUG, "VERSION : %d", num);
					dmsg->version = num;
					state = DYN_SPACES_BEFORE_STAR;
				} else {
					goto error;
				}
			}

			break;

		case DYN_SPACES_BEFORE_STAR:
			//log_debug(LOG_DEBUG, "DYN_CRLF_BEFORE_STAR");
			if (ch == ' ')  {
				break;
			} else if (ch == '*') {
				state = DYN_DATA_LEN;
				num = 0;
			} else {
				goto error;
			}

			//else {
			//	state = DYN_STAR;
			//}

			break;

		//case DYN_STAR:
			//log_debug(LOG_DEBUG, "DYN_STAR");
		//	if (ch == '*') {
		//		state = DYN_DATA_LEN;
		//		num = 0;
		//	} else {
		//		goto error;
		//	}

		//	break;

		case DYN_DATA_LEN:
			//log_debug(LOG_DEBUG, "DYN_DATA_LEN");
			//log_debug(LOG_DEBUG, "num = %d", num);
			if (isdigit(ch))  {
				num = num*10 + (ch - '0');
			} else {
				if (ch == ' ')  {
					log_debug(LOG_DEBUG, "Data len: %d", num);
					dmsg->mlen = num;
					state = DYN_SPACE_BEFORE_DATA;
					num = 0;
				} else {
					goto error;
				}
			}
			break;

		case DYN_SPACE_BEFORE_DATA:
			//log_debug(LOG_DEBUG, "DYN_SPACE_BEFORE_DATA");
			state = DYN_DATA;
			break;

		case DYN_DATA:
			log_debug(LOG_DEBUG, "DYN_DATA");
			p -= 1;
			if (dmsg->mlen > 0)  {
				dmsg->data = p;
				p += dmsg->mlen - 1;
				state = DYN_SPACES_BEFORE_PAYLOAD_LEN;
			} else {
				goto error;
			}

			break;

		case DYN_SPACES_BEFORE_PAYLOAD_LEN: //this only need in dynomite's custome msg
			log_debug(LOG_DEBUG, "DYN_SPACES_BEFORE_PAYLOAD_LEN");
			if (ch == ' ') {
				break;
			} else if (ch == '*') {
				state = DYN_PAYLOAD_LEN;
				num = 0;
			} else {
				goto error;
			}

			break;

		case DYN_PAYLOAD_LEN:
			if (isdigit(ch))  {
				num = num*10 + (ch - '0');
			} else {
				if (ch == CR)  {
					log_debug(LOG_DEBUG, "Data len: %d", num);
					dmsg->plen = num;
					state = DYN_CRLF_BEFORE_DONE;
					num = 0;
				} else {
					goto error;
				}
			}
			break;

		case DYN_CRLF_BEFORE_DONE:
			//log_debug(LOG_DEBUG, "DYN_CRLF_BEFORE_DONE");
			if (*p == LF) {
				state = DYN_DONE;
			} else {
				goto error;
			}

			break;

		case DYN_DONE:
			//log_debug(LOG_DEBUG, "DYN_DONE");
			r->pos = p;
			dmsg->payload = p;
			r->dyn_state = DYN_DONE;
			b->pos = p;
			goto done;
			break;

		default:
			NOT_REACHED();
			break;

		}

	}

	done:

	dmsg->owner = r;
	dmsg->source_address = r->owner->addr;
    //r->mlen = mbuf_length(b);
	//log_debug(LOG_DEBUG, "at done with p at %d", p);
	dmsg_dump(r->dmsg);
	log_hexdump(LOG_VERB, b->pos, mbuf_length(b), "dyn: parsed req %"PRIu64" res %d "
			"type %d state %d rpos %d of %d", r->id, r->result, r->type,
			r->dyn_state, r->pos - b->pos, b->last - b->pos);


	return true;

	skip:
	//log_debug(LOG_DEBUG, "This is not a dyn message");
	dmsg->type = DMSG_UNKNOWN;
	dmsg->owner = r;
	dmsg->source_address = r->owner->addr;
	return true;

	error:
	log_debug(LOG_ERR, "at error");
	r->result = MSG_PARSE_ERROR;
	r->state = state;
	errno = EINVAL;

	log_hexdump(LOG_INFO, b->pos, mbuf_length(b), "parsed bad req %"PRIu64" "
			"res %d type %d state %d", r->id, r->result, r->type,
			r->state);
	return false;

	return true;    //fix me
}




void
dyn_parse_req(struct msg *r)
{
	if (dyn_parse_core(r)) {
		struct dmsg *dmsg = r->dmsg;

        if (dmsg->type == GOSSIP_SYN) {
			log_debug(LOG_DEBUG, "Req parser: I got a GOSSIP_SYN msg");
			r->state = 0;
			r->result = MSG_PARSE_OK;
			r->dyn_state = DYN_DONE;

			//TODOs: need to address multi-buffer msg later
			struct mbuf *b = STAILQ_LAST(&r->mhdr, mbuf, next);
			//dmsg->plen = mbuf_length(b);
			dmsg->payload = b->pos;
            b->pos = b->pos + dmsg->plen;
            r->pos = b->pos;
			//if (b->last < b->end) {  //still other data there for subsequence msg.
			//	r->result = MSG_PARSE_REPAIR;
			//}

			return;
        }

		if (dmsg->type != DMSG_UNKNOWN && dmsg->type != DMSG_REQ) {
			log_debug(LOG_DEBUG, "Req parser: I got a dnode msg of type %d", dmsg->type);
			r->state = 0;
			r->result = MSG_PARSE_OK;
			r->dyn_state = DYN_DONE;
			return;
		}

        if (dmsg->type == CRYPTO_HANDSHAKE) {
			log_debug(LOG_DEBUG, "Req parser: I got a crypto handshake msg");
			r->state = 0;
			r->result = MSG_PARSE_OK;
			r->dyn_state = DYN_DONE;
			return;
        }

		if (r->redis)
			return redis_parse_req(r);

		return memcache_parse_req(r);
	}

	//bad case
	log_debug(LOG_DEBUG, "Bad message - cannot parse");  //fix me to do something
	msg_dump(r);
}


void dyn_parse_rsp(struct msg *r)
{
	if (dyn_parse_core(r)) {
		struct dmsg *dmsg = r->dmsg;

		if (dmsg->type != DMSG_UNKNOWN && dmsg->type != DMSG_REQ) {
			log_debug(LOG_DEBUG, "Resp parser: I got a dnode msg of type %d", dmsg->type);
			r->state = 0;
			r->result = MSG_PARSE_OK;
			r->dyn_state = DYN_DONE;
			return;
		}

		if (r->redis)
			return redis_parse_rsp(r);

		return memcache_parse_rsp(r);
	}

	//bad case
	log_debug(LOG_DEBUG, "Bad message - cannot parse");  //fix me to do something
	msg_dump(r);

	//r->state = 0;
	//r->result = MSG_PARSE_OK;
}


void
dmsg_free(struct dmsg *dmsg)
{
    log_debug(LOG_VVERB, "free dmsg %p id %"PRIu64"", dmsg, dmsg->id);
    dn_free(dmsg);
}


void
dmsg_put(struct dmsg *dmsg)
{
    log_debug(LOG_VVERB, "put dmsg %p id %"PRIu64"", dmsg, dmsg->id);

    nfree_dmsgq++;
    TAILQ_INSERT_HEAD(&free_dmsgq, dmsg, m_tqe);
}

void
dmsg_dump(struct dmsg *dmsg)
{
    struct mbuf *mbuf;

    log_debug(LOG_DEBUG, "dmsg dump: id %"PRIu64" version %d type %d len %"PRIu32"  ", dmsg->id, dmsg->version, dmsg->type, dmsg->mlen);
    //loga_hexdump(dmsg->data, dmsg->mlen, "dmsg with %ld bytes of data", dmsg->mlen);
}


void
dmsg_init(void)
{
    log_debug(LOG_DEBUG, "dmsg size %d", sizeof(struct dmsg));
    dmsg_id = 0;
    nfree_dmsgq = 0;
    TAILQ_INIT(&free_dmsgq);
}





void
dmsg_deinit(void)
{
    struct dmsg *msg, *nmsg;

    for (msg = TAILQ_FIRST(&free_dmsgq); msg != NULL;
         msg = nmsg, nfree_dmsgq--) {
        ASSERT(nfree_dmsgq > 0);
        nmsg = TAILQ_NEXT(msg, m_tqe);
        dmsg_free(msg);
    }
    ASSERT(nfree_dmsgq == 0);
}

bool
dmsg_empty(struct dmsg *msg)
{
    return msg->mlen == 0 ? true : false;
}


struct dmsg *
dmsg_get(void)
{
    struct dmsg *dmsg;

    if (!TAILQ_EMPTY(&free_dmsgq)) {
        ASSERT(nfree_dmsgq > 0);

        dmsg = TAILQ_FIRST(&free_dmsgq);
        nfree_dmsgq--;
        TAILQ_REMOVE(&free_dmsgq, dmsg, m_tqe);
        goto done;
    }

    dmsg = dn_alloc(sizeof(*dmsg));
    if (dmsg == NULL) {
        return NULL;
    }

done:
   // STAILQ_INIT(&dmsg->mhdr);
    dmsg->mlen = 0;
    dmsg->data = NULL;

    dmsg->plen = 0;
    dmsg->payload = NULL;

    dmsg->type = DMSG_UNKNOWN;
    dmsg->version = VERSION_10;
    dmsg->id = 0;
    dmsg->source_address = NULL;
    dmsg->owner = NULL;
    
    return dmsg;
}


rstatus_t 
dmsg_write(struct mbuf *mbuf, uint64_t msg_id, uint8_t type, uint8_t version, struct conn *conn)
{

    mbuf_write_string(mbuf, &MAGIC_STR);
    mbuf_write_uint64(mbuf, msg_id);

    mbuf_write_char(mbuf, ' ');
    mbuf_write_uint8(mbuf, type);

    mbuf_write_char(mbuf, ' ');
    //encryption bit
    if (conn->dnode_secured) {
    	mbuf_write_uint8(mbuf, 1);
    } else {
    	mbuf_write_uint8(mbuf, 0);
    }

    mbuf_write_char(mbuf, ' ');
    mbuf_write_uint8(mbuf, version);

    //mbuf_write_string(mbuf, &CRLF_STR);
    mbuf_write_char(mbuf, ' ');
    mbuf_write_char(mbuf, '*');

    //write aes key
    unsigned char *aes_key = NULL;
    if (conn->dnode_secured) {
        aes_key = generate_aes_key();
        mbuf_write_uint32(mbuf, strlen(aes_key));
    } else {
        mbuf_write_uint32(mbuf, 1);
    }

    mbuf_write_char(mbuf, ' ');
    //mbuf_write_string(mbuf, data);
    if (conn->dnode_secured) {
       mbuf_write_bytes(mbuf, aes_key);
    } else {
       mbuf_write_char(mbuf, 'a'); //TODOs: replace with another string
    }

    //mbuf_write_string(mbuf, &CRLF_STR);
    mbuf_write_char(mbuf, ' ');
    mbuf_write_char(mbuf, '*');
    mbuf_write_uint32(mbuf, 0);
    mbuf_write_string(mbuf, &CRLF_STR);

    log_hexdump(LOG_VERB, mbuf->pos, mbuf_length(mbuf), "dyn message producer: ");
     
    return DN_OK;
}

rstatus_t
dmsg_write_mbuf(struct mbuf *mbuf, uint64_t msg_id, uint8_t type, uint8_t version, struct conn *conn, uint32_t plen)
{
    mbuf_write_string(mbuf, &MAGIC_STR);
    mbuf_write_uint64(mbuf, msg_id);
    mbuf_write_char(mbuf, ' ');
    mbuf_write_uint8(mbuf, type);
    mbuf_write_char(mbuf, ' ');

    //encryption bit
    if (conn->dnode_secured) {
    	mbuf_write_uint8(mbuf, 1);
    } else {
    	mbuf_write_uint8(mbuf, 0);
    }

    mbuf_write_char(mbuf, ' ');
    mbuf_write_uint8(mbuf, version);
    //mbuf_write_string(mbuf, &CRLF_STR);
    mbuf_write_char(mbuf, ' ');
    mbuf_write_char(mbuf, '*');

    //write aes key
    unsigned char *aes_key = NULL;
    if (conn->dnode_secured) {
    	aes_key = generate_aes_key();
    	mbuf_write_uint32(mbuf, strlen(aes_key));
    } else {
        mbuf_write_uint32(mbuf, 1);
    }

    mbuf_write_char(mbuf, ' ');
    //mbuf_write_mbuf(mbuf, data);
    if (conn->dnode_secured) {
       mbuf_write_bytes(mbuf, aes_key);
    } else {
       mbuf_write_char(mbuf, 'a'); //TODOs: replace with another string
    }

    //mbuf_write_string(mbuf, &CRLF_STR);
    mbuf_write_char(mbuf, ' ');
    mbuf_write_char(mbuf, '*');
    mbuf_write_uint32(mbuf, plen);

    mbuf_write_string(mbuf, &CRLF_STR);

    log_hexdump(LOG_VERB, mbuf->pos, mbuf_length(mbuf), "dyn message (writer):  ");

    return DN_OK;
}


static rstatus_t
dmsg_to_gossip(struct ring_msg *rmsg)
{
        CBUF_Push(C2G_InQ, rmsg);

        return DN_OK;
}

static void
dmsg_parse_host_id(uint8_t *start, uint32_t len,
		struct string *dc, struct string *rack, struct dyn_token *token)
{
	uint8_t *p, *q;
	uint8_t *dc_p, *rack_p, *token_p;
	uint32_t k, delimlen, dc_len, rack_len, token_len;
	char delim[] = "$$";
	delimlen = 2;

	/* parse "dc$rack$token : don't support vnode for now */
	//log_hexdump(LOG_VERB, start, len, "host_addr testing: ");
	p = start + len - 1;
	dc_p = NULL;
	rack_p = NULL;
	token_p = NULL;

	dc_len = rack_len = token_len = 0;

	for (k = 0; k < sizeof(delim)-1; k++) {
		q = dn_strrchr(p, start, delim[k]);

		switch (k) {
		case 0:
			//no support for vnode at this time
			token_p = q + 1;
			token_len = (uint32_t)(p - token_p + 1);
			parse_dyn_token(token_p, token_len, token);
			break;
		case 1:
			rack_p = q + 1;
			rack_len = (uint32_t)(p - rack_p + 1);

			string_copy(rack, rack_p, rack_len);
			break;

		default:
			NOT_REACHED();
		}
		p = q - 1;
	}

	if (k != delimlen) {
		loga("Error: this should not happen");
		return;// DN_ERROR;
	}

	dc_p = start;
	dc_len = len - (token_len + rack_len + 2);
	string_copy(dc, dc_p, dc_len);

}


static struct ring_msg *
dmsg_parse(struct dmsg *dmsg)
{
	//rstatus_t status;
	uint8_t *p, *q, *start, *end, *pipe_p;
	uint8_t *host_id, *host_addr, *ts, *node_state;
	uint32_t k, delimlen, host_id_len, host_addr_len, ts_len, node_state_len;
	char delim[] = ",,,";
	delimlen = 3;


	/* parse "host_id1,generation_ts1,host_state1,host_broadcast_address1|host_id2,generation_ts2,host_state2,host_broadcast_address2" */
	/* host_id = dc-rack-token */
	//p = dmsg->data + dmsg->mlen - 1;
	//p = dmsg->owner->pos + dmsg->owner->mlen - 1;
	p = dmsg->payload + dmsg->plen - 1;
	end = p;

	//start = dmsg->data;
	//start = dmsg->owner->pos;
	start = dmsg->payload;

	log_debug(LOG_VERB, "\tparsing msggggggggggggggggg '%.*s'", dmsg->plen, start);
	host_id = NULL;
	host_addr = NULL;
	ts = NULL;
	node_state = NULL;

	host_id_len = 0;
	host_addr_len = 0;
	ts_len = 0;
	node_state_len = 0;
	pipe_p = start;
	int count = 0;

	do {
		q = dn_strrchr(p, start, '|');
		count++;
		p = q - 1;
	} while (q != NULL);

	struct ring_msg *ring_msg = create_ring_msg_with_size(count, true);
	if (ring_msg == NULL) {
		log_debug(LOG_ERR, "Error: unable to create a new ring msg!");
		//we just drop this msg
		return NULL;
	}

	struct server_pool *sp = (struct server_pool *) dmsg->owner->owner->owner;
	ring_msg->sp = sp;
	ring_msg->cb = gossip_msg_peer_update;

	count = 0;
	//p = dmsg->data + dmsg->mlen - 1;
    p = dmsg->payload + dmsg->plen - 1;

	do {

		for (k = 0; k < sizeof(delim)-1; k++) {
			q = dn_strrchr(p, start, delim[k]);

			switch (k) {
			case 0:
				host_addr = q + 1;
				host_addr_len = (uint32_t)(p - host_addr + 1);
				break;
			case 1:
				node_state = q + 1;
				node_state_len = (uint32_t)(p - node_state + 1);

				break;
			case 2:
				ts = q + 1;
				ts_len = (uint32_t)(p - ts + 1);

				break;

			default:
				NOT_REACHED();
			}
			p = q - 1;

		}

		if (k != delimlen) {
			loga("Error: this is insanely bad");
			return NULL;// DN_ERROR;
		}

		pipe_p = dn_strrchr(p, start, '|');

		if (pipe_p == NULL) {
			pipe_p = start;
		} else {
			pipe_p = pipe_p + 1;
			p = pipe_p - 2;
		}

		//host_id = dmsg->data;
		//host_id_len = dmsg->mlen - (host_addr_len + node_state_len + ts_len + 3);
		host_id = pipe_p;
		host_id_len = end - pipe_p - (host_addr_len + node_state_len + ts_len + 3) + 1;

		end = p;

		//log_hexdump(LOG_VERB, host_id, host_id_len, "host_id: ");
		//log_hexdump(LOG_VERB, ts, ts_len, "ts: ");
		//log_hexdump(LOG_VERB, node_state, node_state_len, "state: ");
		//log_hexdump(LOG_VERB, host_addr, host_addr_len, "host_addr: ");

		//log_debug(LOG_VERB, "\t\t host_id          : '%.*s'", host_id_len, host_id);
		//log_debug(LOG_VERB, "\t\t ts               : '%.*s'", ts_len, ts);
		//log_debug(LOG_VERB, "\t\t node_state          : '%.*s'", node_state_len, node_state);
		//log_debug(LOG_VERB, "\t\t host_addr          : '%.*s'", host_addr_len, host_addr);

		struct node *rnode = (struct node *) array_get(&ring_msg->nodes, count);
		dmsg_parse_host_id(host_id, host_id_len, &rnode->dc, &rnode->rack, &rnode->token);

		//print_dyn_token(&rnode->token, 5);
		string_copy(&rnode->name, host_addr, host_addr_len);
		string_copy(&rnode->pname, host_addr, host_addr_len); //need to add port

		rnode->port = sp->d_port;
		rnode->is_local = false;
		rnode->is_seed = false;

		ts[ts_len] = '\0';
		rnode->ts = atol(ts);

		node_state[node_state_len] = '\0';
		rnode->state = (uint8_t) atoi(node_state);

		count++;
	} while (pipe_p != start);

	//TODOs: should move this outside
	dmsg_to_gossip(ring_msg);

	return ring_msg;
}


/*
 * return : true to bypass all processing from the stack
 *          false to go through the whole stack to process a given msg
 */
bool
dmsg_process(struct context *ctx, struct conn *conn, struct dmsg *dmsg)
{
    ASSERT(dmsg != NULL);
    ASSERT(conn->dyn_mode);

    struct string s;

    log_debug(LOG_DEBUG, "dmsg process: type %d", dmsg->type);
    switch(dmsg->type) {
        case DMSG_DEBUG:
           s.len = dmsg->mlen;
           s.data = dmsg->data;
           log_hexdump(LOG_VERB, s.data, s.len, "dyn processing message ");
           return true;

        case GOSSIP_SYN:
           log_debug(LOG_DEBUG, "I have got a GOSSIP_SYN!!!!!!");
           dmsg_dump(dmsg);
           //TODOs: fix this to reply the 1st time sender
           //dnode_rsp_gos_syn(ctx, conn, dmsg->owner);
           dmsg_parse(dmsg);
           return true;

        case CRYPTO_HANDSHAKE:
        	log_debug(LOG_DEBUG, "I have a crypto handshake msg and processing it now");
            //TODOs: will work on this to optimize the performance
        	return true;

        case GOSSIP_SYN_REPLY:
           log_debug(LOG_DEBUG, "I have got a GOSSIP_SYN_REPLY!!!!!!");

           return true;
        default:
           log_debug(LOG_DEBUG, "nothing to do");
    }
       
    return false;
}


