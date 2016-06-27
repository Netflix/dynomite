/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 

#include <ctype.h>

#include "dyn_core.h"
#include "dyn_crypto.h"
#include "dyn_dnode_msg.h"
#include "dyn_server.h"
#include "proto/dyn_proto.h"

static uint8_t version = VERSION_10;

static uint64_t dmsg_id;          /* message id counter */
static uint32_t nfree_dmsgq;      /* # free msg q */
static struct dmsg_tqh free_dmsgq; /* free msg q */
static pthread_spinlock_t lock;

static const struct string MAGIC_STR = string("   $2014$ ");
static const struct string CRLF_STR = string(CRLF);

static unsigned char aes_encrypted_buf[130];
static unsigned char aes_decrypted_buf[34];

static rstatus_t dmsg_to_gossip(struct ring_msg *rmsg);


const char *
dyn_state_get_string(dyn_parse_state_t dyn_state)
{
    switch (dyn_state) {
        case DYN_START: return "DYN_START";
        case DYN_MAGIC_STRING: return "DYN_MAGIC_STRING";
        case DYN_MAGIC_STRING_SPACE: return "DYN_MAGIC_STRING_SPACE";
        case DYN_MSG_ID: return "DYN_MSG_ID";
        case DYN_MSG_ID_SPACE: return "DYN_MSG_ID_SPACE";
        case DYN_TYPE_ID: return "DYN_TYPE_ID";
        case DYN_TYPE_ID_SPACE: return "DYN_TYPE_ID_SPACE";
        case DYN_BIT_FIELD: return "DYN_BIT_FIELD";
        case DYN_BIT_FIELD_SPACE: return "DYN_BIT_FIELD_SPACE";
        case DYN_VERSION: return "DYN_VERSION";
        case DYN_VERSION_SPACE: return "DYN_VERSION_SPACE";
        case DYN_SAME_DC: return "DYN_SAME_DC";
        case DYN_SAME_DC_SPACE: return "DYN_SAME_DC_SPACE";
        case DYN_AESKEY_LEN_STAR: return "DYN_AESKEY_LEN_STAR";
        case DYN_AESKEY_LEN: return "DYN_AESKEY_LEN";
        case DYN_AESKEY: return "DYN_AESKEY";
        case DYN_PAYLOAD_LEN_STAR: return "DYN_PAYLOAD_LEN_STAR";
        case DYN_PAYLOAD_LEN: return "DYN_PAYLOAD_LEN";
        case DYN_CR_BEFORE_DONE: return "DYN_CR_BEFORE_DONE";
        case DYN_LF_BEFORE_DONE: return "DYN_LF_BEFORE_DONE";
        case DYN_DONE: return "DYN_DONE";
        case DYN_POST_DONE: return "DYN_POST_DONE";
        case DYN_UNKNOWN: return "DYN_UNKNOWN";
        default: return "UNSPECIFIED";
    }
}

#define PRINT_WEIRD_CHAR_AND_RESET(dyn_state, ch)           \
        {                                                   \
            loga("%s weird char (%d) '%c'",                 \
                 dyn_state_get_string(dyn_state), ch, ch);  \
            token = NULL;                                   \
            dyn_state = DYN_START;                          \
            if (ch == '$')                                  \
               p--;                                         \
        }

/* The function parses a dnode header. The dnode header is same for the
 * request as well as response.
 * Here we start parsing and look for the magic string and then the state
 * machine starts. However when we encounter a weird character, we should
 * reset the state to DYN_START, and also reset the fields in the dmsg that
 * we have been accumulating.
 * There are chances that we have only some part of the header, at which
 * point, we keep the partial header and reuse it in the next parsing.
 * The partial header is kept in the message itself.
 */
static bool 
dyn_parse_core(struct msg *r)
{
   struct dmsg *dmsg;
   struct mbuf *b;
   uint8_t ch = ' ';
   uint64_t num = 0;
   bool log = log_loggable(LOG_VVERB);
   //bool log = !r->request;

   dyn_parse_state_t dyn_state = r->dyn_state;
   if (log) {
      log_warn("%s: starting to parse", dyn_state_get_string(dyn_state), r->dyn_state);
      msg_dump(r);
   }

   if (r->dyn_state == DYN_DONE || r->dyn_state == DYN_POST_DONE) {
       if (log)
           log_warn("Dyn header of message %lu dmsg id %lu is already parsed",
                    r->id, r->dmsg ? r->dmsg->id : 0);
       return true;
   }

   b = STAILQ_LAST(&r->mhdr, mbuf, next);

   dmsg = r->dmsg;
   if (dmsg == NULL) {
       dmsg = r->dmsg = dmsg_get();
       dmsg->owner = r;
       if (dmsg == NULL) {//should track this as a dropped message
           loga("unable to create a new dmsg");
           log_error("Unable to create dmsg. at error for state %s",
                   dyn_state_get_string(dyn_state));
           r->result = MSG_PARSE_ERROR;
           errno = DN_ENOMEM;
           return false;
       }
   }

   bool reset_dmsg = false;
   uint8_t *token = NULL;
   uint8_t *p;
   for (p = r->pos; p < b->last; p++) {
       if (reset_dmsg) {
           dmsg->mlen = 0;
           dmsg->data = NULL;
           dmsg->plen = 0;
           dmsg->payload = NULL;
           dmsg->type = DMSG_UNKNOWN;
           dmsg->version = VERSION_10;
           dmsg->id = 0;
           dmsg->source_address = NULL;
           dmsg->owner = r;
           dmsg->bit_field = 0;
           dmsg->same_dc = 1;
           reset_dmsg = false;
       }
      ch = *p;
      if (log) {
         log_warn("%s: ch: (%d) '%c'", dyn_state_get_string(dyn_state), ch, ch);
      }
      switch (dyn_state) {
      case DYN_START:
         if (ch != ' ' && ch != '$') {
             // keep looking for the token
             continue;
         }

         if (ch == ' ') {
            if (!token)
               token = p;
            continue;
         }

         if (ch == '$') {
             if (!token)
                 token = p;
              if (p + 5 < b->last) {
                  if ((*(p+1) == '2') &&
                      (*(p+2) == '0') &&
                      (*(p+3) == '1') &&
                      (*(p+4) == '4') &&
                      (*(p+5) == '$')) {
                      if (log)
                        log_warn("%s, new state DYN_MAGIC_STRING_SPACE",
                                 dyn_state_get_string(dyn_state));
                      dyn_state = DYN_MAGIC_STRING_SPACE;
                     p += 5;
                     break;
                  } else {
                     //goto skip;
                     reset_dmsg = true;
                     token = NULL; //reset
                     continue;
                  }
              } else {
                    // The token is split across two messages,
                    goto split;
              }
         } else {
            loga("%s weird char (%d) '%c'", dyn_state_get_string(dyn_state), ch, ch);
            //goto skip;
            reset_dmsg = true;
            token = NULL; //reset
            continue;
         }
         break;

      case DYN_MAGIC_STRING_SPACE:
         if (ch == ' ') {
            dyn_state = DYN_MSG_ID;
            num = 0;
            continue;
         } else {
            loga("%s Facing a weird char (%d) '%c'", dyn_state_get_string(dyn_state), ch, ch);
            token = NULL;
            dyn_state = DYN_START;
            reset_dmsg = true;
            if (ch == '$')
                p--;
            continue;
         }
         break;

      case DYN_MSG_ID:
         if (isdigit(ch))  {
            num = num*10 + (uint64_t)(ch - '0');
            continue;
         }
         if (log) {
             log_warn("%s MSG ID : %d new state DYN_MSG_ID_SPACE",
                      dyn_state_get_string(dyn_state), num);
         }
         dmsg->id = num;
         dyn_state = DYN_MSG_ID_SPACE;
         p--;
         num = 0;
         continue;

      case DYN_MSG_ID_SPACE:
         if (ch == ' ') {
            dyn_state = DYN_TYPE_ID;
            continue;
         } else {
            loga("%s weird char (%d) '%c'", dyn_state_get_string(dyn_state), ch, ch);
            token = NULL; //reset, and start from beginning
            dyn_state = DYN_START;
            reset_dmsg = true;
            if (ch == '$')
               p--;
         }
         break;

      case DYN_TYPE_ID:
         if (isdigit(ch))  {
            num = num*10 + (uint64_t)(ch - '0');
            continue;
         }
         if (log) {
             log_warn("%s TYPE ID : %d new state DYN_TYPE_ID_SPACE",
                     dyn_state_get_string(dyn_state), num);
         }
         dmsg->type = num;
         dyn_state = DYN_TYPE_ID_SPACE;
         num = 0;
         p--;
         continue;
      case DYN_TYPE_ID_SPACE:
        if (ch == ' ') {
            dyn_state = DYN_BIT_FIELD;
            continue;
         } else {
            loga("%s weird char (%d) '%c'", dyn_state_get_string(dyn_state), ch, ch);
            token = NULL; //reset, and start from beginning
            dyn_state = DYN_START;
            reset_dmsg = true;
            if (ch == '$') // backtrack one characted to get the '$' token
               p--;
         }
         break;
      case DYN_BIT_FIELD:
         if (isdigit(ch))  {
            num = num*10 + (uint64_t)(ch - '0');
            if (log) {
                log_warn("%s Encryption: %d new state DYN_BIT_FIELD_SPACE",
                         dyn_state_get_string(dyn_state), num);
            }
            dmsg->bit_field = num & 0xF;
            dyn_state = DYN_BIT_FIELD_SPACE;
            continue;
         } else {
            loga("%s weird char (%d) '%c'", dyn_state_get_string(dyn_state), ch, ch);
            token = NULL; //reset, and start from beginning
            dyn_state = DYN_START;
            reset_dmsg = true;
            if (ch == '$') // backtrack one characted to get the '$' token
               p--;
         }
         break;
      case DYN_BIT_FIELD_SPACE:
         if (ch == ' ') {
             dyn_state = DYN_VERSION;
             continue;
         } else {
            loga("%s weird char (%d) '%c'", dyn_state_get_string(dyn_state), ch, ch);
            token = NULL; //reset, and start from beginning
            dyn_state = DYN_START;
            reset_dmsg = true;
            if (ch == '$') // backtrack one characted to get the '$' token
               p--;
         }
         break;
      case DYN_VERSION:
         if (isdigit(ch))  {
            num = num*10 + (uint64_t)(ch - '0');
            continue;
         }
         if (log) {
             log_warn("%s VERSION: %d new state DYN_VERSION_SPACE",
                     dyn_state_get_string(dyn_state), num);
         }
         dmsg->version = num;
         dyn_state = DYN_VERSION_SPACE;
         p--;
         num = 0;
         continue;
      case DYN_VERSION_SPACE:
         if (ch == ' ') {
            dyn_state = DYN_SAME_DC;
            num = 0;
            continue;
         } else {
            loga("%s weird char (%d) '%c'", dyn_state_get_string(dyn_state), ch, ch);
            token = NULL;
            dyn_state = DYN_START;
            reset_dmsg = true;
            if (ch == '$')
               p--;
         }
         break;
      case DYN_SAME_DC:
         if (isdigit(ch)) {
             dmsg->same_dc = (uint8_t)(ch - '0');
             if (log) {
                 log_warn("%s same_dc %d new state DYN_SAME_DC_SPACE",
                         dyn_state_get_string(dyn_state), dmsg->same_dc);
             }
             dyn_state = DYN_SAME_DC_SPACE;
             continue;
         } else {
             loga("%s weird char (%d) '%c'", dyn_state_get_string(dyn_state), ch, ch);
             token = NULL;
             dyn_state = DYN_START;
            reset_dmsg = true;
             if (ch == '$')
                 p--;
         }
         continue;
      case DYN_SAME_DC_SPACE:
         if (ch == ' ') {
             dyn_state = DYN_AESKEY_LEN_STAR;
             num = 0;
         } else {
             loga("%s weird char (%d) '%c'", dyn_state_get_string(dyn_state), ch, ch);
             token = NULL;
             dyn_state = DYN_START;
            reset_dmsg = true;
             if (ch == '$')
      		   p--;
      	}
      	break;

    case DYN_AESKEY_LEN_STAR:
         if (ch == '*') {
             dyn_state = DYN_AESKEY_LEN;
             num = 0;
         } else {
             loga("%s weird char (%d) '%c'", dyn_state_get_string(dyn_state), ch, ch);
             token = NULL;
             dyn_state = DYN_START;
            reset_dmsg = true;
             if (ch == '$')
      		   p--;
      	}
      	break;

      case DYN_AESKEY_LEN:
         if (isdigit(ch))  {
            num = num*10 + (uint64_t)(ch - '0');
            continue;
         }
         if (log) {
             log_warn("%s AES key len: %u new state DYN_AESKEY_LEN_SPACE",
                      dyn_state_get_string(dyn_state), num);
         }
         dmsg->mlen = (uint32_t)num;
         dyn_state = DYN_AESKEY_LEN_SPACE;
         num = 0;
         p--;
         continue;

      case DYN_AESKEY_LEN_SPACE:
         if (ch == ' ') {
            dyn_state = DYN_AESKEY;
            num = 0;
         } else {
            loga("%s weird char (%d) '%c'", dyn_state_get_string(dyn_state), ch, ch);
            token = NULL;
            dyn_state = DYN_START;
            reset_dmsg = true;
            if (ch == '$')
               p--;
         }
         break;
      case DYN_AESKEY:
        if (p + dmsg->mlen < b->last) {
            dmsg->data = p;
            p += dmsg->mlen - 1;
            dyn_state = DYN_AESKEY_SPACE;
            continue;
         } else {
            goto split;
         }
         break;
      case DYN_AESKEY_SPACE:
         if (ch == ' ') {
             dyn_state = DYN_PAYLOAD_LEN_STAR;
             num = 0;
         } else {
            loga("%s weird char (%d) '%c'",
                 dyn_state_get_string(dyn_state), ch, ch);
            token = NULL;
            dyn_state = DYN_START;
            reset_dmsg = true;
            if (ch == '$')
               p--;
         }
         break;

      case DYN_PAYLOAD_LEN_STAR:
         if (ch == '*') {
             dyn_state = DYN_PAYLOAD_LEN;
             num = 0;
         } else {
             loga("%s weird char (%d) '%c'", dyn_state_get_string(dyn_state), ch, ch);
             token = NULL;
             dyn_state = DYN_START;
            reset_dmsg = true;
             if (ch == '$')
      		   p--;
      	}
      	break;

      case DYN_PAYLOAD_LEN:
        if (isdigit(ch))  {
            num = num*10 + (uint64_t)(ch - '0');
            continue;
         }
         if (log) {
             log_warn("%s payload len: %u new state DYN_CR_BEFORE_DONE",
                      dyn_state_get_string(dyn_state), num);
         }
         dmsg->plen = (uint32_t)num;
         dyn_state = DYN_CR_BEFORE_DONE;
         num = 0;
         p--;
         continue;
      case DYN_CR_BEFORE_DONE:
         if (ch == CR)
             dyn_state = DYN_LF_BEFORE_DONE;
         else {
            loga("%s weird char (%d) '%c'", dyn_state_get_string(dyn_state), ch, ch);
            token = NULL;
            dyn_state = DYN_START;
            reset_dmsg = true;
            if (ch == '$')
               p--;
         }
         break;

      case DYN_LF_BEFORE_DONE:
         if (ch == LF) {
             dyn_state = DYN_DONE;
         } else {
            loga("%s weird char (%d) '%c'", dyn_state_get_string(dyn_state), ch, ch);
            token = NULL;
            dyn_state = DYN_START;
            reset_dmsg = true;
            if (ch == '$')
               p--;
         }
         break;

      case DYN_DONE:
         // Store everything in msg r
         r->pos = p;
         dmsg->payload = p;
         r->dyn_state = DYN_DONE;
         b->pos = p;
         goto done;
         break;

      default:
         ASSERT_LOG(NULL, "dyn_state %d", dyn_state);
         NOT_REACHED();
         break;

      }

   }

   if (log_loggable(LOG_DEBUG)) {
      log_debug(LOG_DEBUG, "Not fully parsed yet!!!!!!");
   }
   split:
   //this is an attempt recovery when we got a bad message
   //we try to look for the start the next good one and throw away the bad part
   if (r->dyn_state == DYN_START) {
      r->result = MSG_PARSE_AGAIN;
       if (b->last == b->end) {
          struct mbuf *nbuf = mbuf_get();
          ASSERT_LOG(nbuf != NULL, "Unable to obtain a new mbuf for replacement!");
          mbuf_insert(&r->mhdr, nbuf);
          mbuf_remove(&r->mhdr, b);
          mbuf_put(b);
          r->pos = nbuf->pos;
          return false;
       } else { //split it and throw away the bad portion
           struct mbuf *nbuf;

           nbuf = mbuf_split(&r->mhdr, r->pos, NULL, NULL);
           ASSERT_LOG(nbuf != NULL, "Unable to obtain a new mbuf");
           mbuf_insert(&r->mhdr, nbuf);
           mbuf_remove(&r->mhdr, b);
           mbuf_put(b);
           r->pos = nbuf->pos;
           return false;
       }
   }

   if (mbuf_length(b) == 0 || b->last == b->end) {
      if (log) {
          log_warn("Would this case ever happen?");
      }
      r->result = MSG_PARSE_AGAIN;
      return false;
   }

   if (r->pos == b->last) {
      if (log) {
           log_warn("Forward to reading the new block of data");
       }
       r->dyn_state = DYN_START;
       r->result = MSG_PARSE_AGAIN;
       token = NULL;
       return false;
   }

   if (log) {
       log_warn("in split");
   }
   r->dyn_state = DYN_START;
   r->pos = token;
   r->result = MSG_PARSE_REPAIR;
   if (log_loggable(LOG_WARN)) {
      log_hexdump(LOG_WARN, b->pos, mbuf_length(b), "split and inspecting req %"PRIu64" "
            "res %d type %d state %d", r->id, r->result, r->type,
            r->dyn_state);

      log_hexdump(LOG_WARN, b->start, b->last - b->start, "split and inspecting full req %"PRIu64" "
            "res %d type %d state %d", r->id, r->result, r->type,
            r->dyn_state);
   }
   return false;

done:
   r->pos = p;
   dmsg->source_address = r->owner->addr;

   if (log_loggable(LOG_VVERB)) {
      log_debug(LOG_VVERB, "at done with p at %d", p);
      log_hexdump(LOG_VVERB, r->pos, b->last - r->pos, "done and inspecting req %"PRIu64" "
            "res %d type %d state %d", r->id, r->result, r->type,
            r->dyn_state);
      log_hexdump(LOG_VVERB, b->start, b->last - b->start, "inspecting req %"PRIu64" "
            "res %d type %d state %d", r->id, r->result, r->type,
            r->dyn_state);
   }

   return true;
}


void
dyn_parse_req(struct msg *r)
{
	if (log_loggable(LOG_VVERB)) {
		log_debug(LOG_VVERB, ":::::::::::::::::::::: In dyn_parse_req, start to process request :::::::::::::::::::::: ");
		msg_dump(r);
	}

	bool done_parsing = false;
	struct mbuf *b = STAILQ_LAST(&r->mhdr, mbuf, next);

	if (dyn_parse_core(r)) {
		struct dmsg *dmsg = r->dmsg;
		struct conn *conn = r->owner;
        if (conn->p.type != CONN_DNODE_PEER_SERVER) {
            if (conn->same_dc != !!dmsg->same_dc)
                log_error("SHAILESH.......................changing same_dc field of conn %p to %d", conn, !!dmsg->same_dc);
            conn->same_dc = !!dmsg->same_dc;
        }

		if (dmsg->type != DMSG_UNKNOWN && dmsg->type != DMSG_REQ &&
				dmsg->type != DMSG_REQ_FORWARD && dmsg->type != GOSSIP_SYN) {
			r->state = 0;
			r->result = MSG_PARSE_OK;
			r->dyn_state = DYN_DONE;
			return;
		}

		if (r->dyn_state == DYN_DONE && dmsg->bit_field == 1) {
			dmsg->owner->owner->dnode_secured = 1;
			r->owner->dnode_crypto_state = 1;
			r->dyn_state = DYN_POST_DONE;
			r->result = MSG_PARSE_REPAIR;

			if (dmsg->mlen > 1) {
				//Decrypt AES key
				dyn_rsa_decrypt(dmsg->data, aes_decrypted_buf);
				strncpy((char*)r->owner->aes_key, (char*)aes_decrypted_buf, strlen((char*)aes_decrypted_buf));
			}

			if (dmsg->plen + b->pos <= b->last) {
				struct mbuf *decrypted_buf = mbuf_get();
				if (decrypted_buf == NULL) {
					loga("Unable to obtain an mbuf for dnode msg's header!");
					r->result = MSG_OOM_ERROR;
					return;
				}

				dyn_aes_decrypt(b->pos, dmsg->plen, decrypted_buf, r->owner->aes_key);

				b->pos = b->pos + dmsg->plen;
				r->pos = decrypted_buf->start;
				mbuf_copy(decrypted_buf, b->pos, mbuf_length(b));

				mbuf_insert(&r->mhdr, decrypted_buf);
				mbuf_remove(&r->mhdr, b);
				mbuf_put(b);

				r->mlen = mbuf_length(decrypted_buf);

				data_store_parse_req(r);

			}

			//substract alraedy received bytes
			dmsg->plen -= (uint32_t)(b->last - b->pos);

			return;
		} else if (r->dyn_state == DYN_POST_DONE) {
			struct mbuf *last_buf = STAILQ_LAST(&r->mhdr, mbuf, next);
			if (last_buf->read_flip == 1) {
				data_store_parse_req(r);
			} else {
				r->result = MSG_PARSE_AGAIN;
			}
			return;
		}

		if (dmsg->type == GOSSIP_SYN) {
			//TODOs: need to address multi-buffer msg later
			dmsg->payload = b->pos;

			b->pos = b->pos + dmsg->plen;
			r->pos = b->pos;

			done_parsing = true;
		}

		if (done_parsing)
			return;

		return data_store_parse_req(r);
	}

	//bad case
	if (log_loggable(LOG_VVERB)) {
		log_debug(LOG_VVERB, "Bad or splitted message");  //fix me to do something
		msg_dump(r);
	}
	r->result = MSG_PARSE_AGAIN;
}

static void
data_store_parse_rsp(struct msg *r)
{
	if (g_data_store == DATA_REDIS) {
		return redis_parse_rsp(r);
	}
	else if (g_data_store == DATA_MEMCACHE){
		return memcache_parse_rsp(r);
	}
	else{
		//if (log_loggable(LOG_VVERB)) {
			//	log_hexdump(LOG_VVERB,"incorrect selection of data store %d (parse request)", data_store);
		//}
		exit(0);
	}
}

void dyn_parse_rsp(struct msg *r)
{
	if (log_loggable(LOG_VVERB)) {
		log_debug(LOG_VVERB, ":::::::::::::::::::::: In dyn_parse_rsp, start to process response :::::::::::::::::::::::: ");
		msg_dump(r);
	}

	struct mbuf *b = STAILQ_LAST(&r->mhdr, mbuf, next);
	if (dyn_parse_core(r)) {
		struct dmsg *dmsg = r->dmsg;
		struct conn *conn = r->owner;
        //if (conn->p.type != CONN_DNODE_PEER_SERVER) {
            if (conn->same_dc != !!dmsg->same_dc)
                log_error("SHAILESH.......................changing same_dc field of conn %p to %d", conn, !!dmsg->same_dc);
            conn->same_dc = !!dmsg->same_dc;
        //}

		if (dmsg->type != DMSG_UNKNOWN && dmsg->type != DMSG_RES) {
			log_debug(LOG_DEBUG, "Resp parser: I got a dnode msg of type %d", dmsg->type);
			r->state = 0;
			r->result = MSG_PARSE_OK;
			r->dyn_state = DYN_DONE;
			return;
		}

		if (r->dyn_state == DYN_DONE && dmsg->bit_field == 1) {
			dmsg->owner->owner->dnode_secured = 1;
			r->owner->dnode_crypto_state = 1;
			r->dyn_state = DYN_POST_DONE;
			r->result = MSG_PARSE_REPAIR;

			if (dmsg->mlen > 1) {
				//Decrypt AES key
				dyn_rsa_decrypt(dmsg->data, aes_decrypted_buf);
				strncpy((char *)r->owner->aes_key, (char *)aes_decrypted_buf,
                        strlen((char *)aes_decrypted_buf));
			}

			if (dmsg->plen + b->pos <= b->last) {
				struct mbuf *decrypted_buf = mbuf_get();
				if (decrypted_buf == NULL) {
					loga("Unable to obtain an mbuf for dnode msg's header!");
					r->result = MSG_OOM_ERROR;
					return;
				}

				dyn_aes_decrypt(b->pos, dmsg->plen, decrypted_buf, r->owner->aes_key);

				b->pos = b->pos + dmsg->plen;
				r->pos = decrypted_buf->start;
				mbuf_copy(decrypted_buf, b->pos, mbuf_length(b));

				mbuf_insert(&r->mhdr, decrypted_buf);
				mbuf_remove(&r->mhdr, b);
				mbuf_put(b);

				r->mlen = mbuf_length(decrypted_buf);

				return data_store_parse_rsp(r);
			}

			//Subtract already received bytes
			dmsg->plen -= (uint32_t)(b->last - b->pos);
			return;

		} else if (r->dyn_state == DYN_POST_DONE) {
			struct mbuf *last_buf = STAILQ_LAST(&r->mhdr, mbuf, next);
			if (last_buf->read_flip == 1) {
				data_store_parse_rsp(r);
			} else {
				r->result = MSG_PARSE_AGAIN;
			}
			return;
		}

		return data_store_parse_rsp(r);
	}

	//bad case
	if (log_loggable(LOG_VVERB)) {
		log_debug(LOG_VVERB, "Resp: bad message - cannot parse");  //fix me to do something
		msg_dump(r);
	}

	r->result = MSG_PARSE_AGAIN;

}


void
dmsg_free(struct dmsg *dmsg)
{
    log_debug(LOG_VVVERB, "free dmsg %p id %"PRIu64"", dmsg, dmsg->id);
    dn_free(dmsg);
}


void
dmsg_put(struct dmsg *dmsg)
{
    if (log_loggable(LOG_VVVERB)) {
        log_debug(LOG_VVVERB, "put dmsg %p id %"PRIu64"", dmsg, dmsg->id);
    }
    int ret = pthread_spin_lock(&lock);
    ASSERT_LOG(!ret, "Failed to lock spin lock. err:%d error: %s", ret, strerror(ret));
    nfree_dmsgq++;
    TAILQ_INSERT_HEAD(&free_dmsgq, dmsg, m_tqe);
    ret = pthread_spin_unlock(&lock);
    ASSERT_LOG(!ret, "Failed to unlock spin lock. err:%d error: %s", ret, strerror(ret));
}

void
dmsg_dump(struct dmsg *dmsg)
{
    log_debug(LOG_VVVERB, "dmsg dump: id %"PRIu64" version %d  bit_field %d type %d len %"PRIu32"  plen %"PRIu32" ",
                 dmsg->id, dmsg->version, dmsg->bit_field, dmsg->type, dmsg->mlen, dmsg->plen);
}


void
dmsg_init(void)
{
    log_debug(LOG_VVVERB, "dmsg size %d", sizeof(struct dmsg));

    dmsg_id = 0;
    nfree_dmsgq = 0;
    int ret = pthread_spin_init(&lock, PTHREAD_PROCESS_PRIVATE);
    ASSERT_LOG(!ret, "Failed to init spin lock. err:%d error: %s", ret, strerror(ret));
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
    int ret = pthread_spin_destroy(&lock);
    ASSERT_LOG(!ret, "Failed to destroy spin lock. err:%d error: %s", ret, strerror(ret));
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

    int ret = pthread_spin_lock(&lock);
    ASSERT_LOG(!ret, "Failed to lock spin lock. err:%d error: %s", ret, strerror(ret));
    if (!TAILQ_EMPTY(&free_dmsgq)) {
        ASSERT(nfree_dmsgq > 0);

        dmsg = TAILQ_FIRST(&free_dmsgq);
        nfree_dmsgq--;
        TAILQ_REMOVE(&free_dmsgq, dmsg, m_tqe);
        ret = pthread_spin_unlock(&lock);
        ASSERT_LOG(!ret, "Failed to unlock spin lock. err:%d error: %s", ret, strerror(ret));
        goto done;
    }

    dmsg = dn_alloc(sizeof(*dmsg));
    ret = pthread_spin_unlock(&lock);
    ASSERT_LOG(!ret, "Failed to unlock spin lock. err:%d error: %s", ret, strerror(ret));
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
    dmsg->bit_field = 0;
    dmsg->same_dc = 1;
 
    return dmsg;
}


rstatus_t 
dmsg_write(struct mbuf *mbuf, uint64_t msg_id, uint8_t type,
         struct conn *conn, uint32_t payload_len)
{

    mbuf_write_string(mbuf, &MAGIC_STR);
    mbuf_write_uint64(mbuf, msg_id);

    //type
    mbuf_write_char(mbuf, ' ');
    mbuf_write_uint8(mbuf, type);

    //bit field
    mbuf_write_char(mbuf, ' ');
    //encryption bit
    if (conn->dnode_secured) {
       mbuf_write_uint8(mbuf, 1);
    } else {
       mbuf_write_uint8(mbuf, 0);
    }

    //version
    mbuf_write_char(mbuf, ' ');
    mbuf_write_uint8(mbuf, version);

    //same-dc
    mbuf_write_char(mbuf, ' ');
    if (conn->same_dc)
        mbuf_write_uint8(mbuf, 1);
    else
        mbuf_write_uint8(mbuf, 0);

    //data
    mbuf_write_char(mbuf, ' ');
    mbuf_write_char(mbuf, '*');

    //write aes key
    unsigned char *aes_key = conn->aes_key;

    if (conn->dnode_secured && conn->dnode_crypto_state == 0) {
        mbuf_write_uint32(mbuf, (uint32_t)dyn_rsa_size());
    } else {
        mbuf_write_uint32(mbuf, 1);
    }

    //payload
    mbuf_write_char(mbuf, ' ');
    //mbuf_write_string(mbuf, data);
    if (conn->dnode_secured && conn->dnode_crypto_state == 0) {
       dyn_rsa_encrypt(aes_key, aes_encrypted_buf);
       mbuf_write_bytes(mbuf, aes_encrypted_buf, dyn_rsa_size());
       conn->dnode_crypto_state = 1;
    } else {
       mbuf_write_char(mbuf, 'd'); //TODOs: replace with another string
    }

    mbuf_write_char(mbuf, ' ');
    mbuf_write_char(mbuf, '*');
    mbuf_write_uint32(mbuf, payload_len);
    mbuf_write_string(mbuf, &CRLF_STR);
     
    return DN_OK;
}

//Used in gossip forwarding msg only for now
rstatus_t
dmsg_write_mbuf(struct mbuf *mbuf, uint64_t msg_id, uint8_t type, struct conn *conn, uint32_t plen)
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

    //same-dc
    mbuf_write_char(mbuf, ' ');
    if (conn->same_dc)
        mbuf_write_uint8(mbuf, 1);
    else
        mbuf_write_uint8(mbuf, 0);

    //mbuf_write_string(mbuf, &CRLF_STR);
    mbuf_write_char(mbuf, ' ');
    mbuf_write_char(mbuf, '*');

    //write aes key
    unsigned char *aes_key = conn->aes_key;
    if (conn->dnode_secured) {
       mbuf_write_uint32(mbuf, (uint32_t)dyn_rsa_size());
    } else {
        mbuf_write_uint32(mbuf, 1);
    }

    mbuf_write_char(mbuf, ' ');
    //mbuf_write_mbuf(mbuf, data);
    if (conn->dnode_secured) {
       dyn_rsa_encrypt(aes_key, aes_encrypted_buf);
       mbuf_write_bytes(mbuf, aes_encrypted_buf, dyn_rsa_size());
    } else {
       mbuf_write_char(mbuf, 'a'); //TODOs: replace with another string
    }

    //mbuf_write_string(mbuf, &CRLF_STR);
    mbuf_write_char(mbuf, ' ');
    mbuf_write_char(mbuf, '*');
    mbuf_write_uint32(mbuf, plen);

    mbuf_write_string(mbuf, &CRLF_STR);

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

   host_id = NULL;
   host_addr = NULL;
   ts = NULL;
   node_state = NULL;

   host_id_len = 0;
   host_addr_len = 0;
   ts_len = 0;
   node_state_len = 0;
   pipe_p = start;
   uint32_t count = 0;

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
      host_id_len = (uint32_t)(end - pipe_p - (host_addr_len + node_state_len + ts_len + 3)
                    + 1);

      end = p;


      struct gossip_node *rnode = (struct gossip_node *) array_get(&ring_msg->nodes, count);
      dmsg_parse_host_id(host_id, host_id_len, &rnode->dc, &rnode->rack, &rnode->token);


      string_copy(&rnode->name, host_addr, host_addr_len);
      string_copy(&rnode->pname, host_addr, host_addr_len); //need to add port

      rnode->port = sp->dnode_proxy_endpoint.port;
      rnode->is_local = false;
      rnode->is_seed = false;

      ts[ts_len] = '\0';
      rnode->ts = (uint64_t)atol((char*)ts);

      node_state[node_state_len] = '\0';
      rnode->state = (uint8_t) atoi((char*)node_state);

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

    switch(dmsg->type) {

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

/*
 *
 */

void
data_store_parse_req(struct msg *r)
{
	if (g_data_store == DATA_REDIS) {
		return redis_parse_req(r);
	}
	else if (g_data_store == DATA_MEMCACHE){
		return memcache_parse_req(r);
	}
	else{
		//if (log_loggable(LOG_VVERB)) {
			//	log_hexdump(LOG_VVERB,"incorrect selection of data store %d (parse request)", data_store);
		//}
		exit(0);
	}
}
