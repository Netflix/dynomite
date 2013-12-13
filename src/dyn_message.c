#include <ctype.h>

#include <nc_core.h>
#include <nc_proto.h>

#include <dyn_message.h>



static uint64_t dmsg_id;          /* message id counter */
static uint32_t nfree_dmsgq;      /* # free msg q */
static struct dmsg_tqh free_dmsgq; /* free msg q */

static uint32_t MAGIC_NUMBER = 2014;

static const struct string MAGIC_STR = string("2014 ");
static const struct string CRLF_STR = string(CRLF);


void
dyn_parse_req(struct msg *r)
{
        struct dmsg *dmsg;
	struct mbuf *b;
	uint8_t *p, *m;
	uint8_t ch;
        uint32_t num = 0;

	enum {
            DYN_START,
            DYN_MAGIC_NUMBER = 1000,
            DYN_SPACES_BEFORE_MSG_ID,
            DYN_MSG_ID,
            DYN_SPACES_BEFORE_TYPE_ID,
            DYN_TYPE_ID,
            DYN_SPACES_BEFORE_VERSION,
            DYN_VERSION,
            DYN_CRLF_BEFORE_STAR,
            DYN_STAR,
            DYN_DATA_LEN,
            DYN_SPACE_BEFORE_DATA,
            DYN_DATA,
            DYN_CRLF_BEFORE_DONE,
            DYN_DONE
	} state;
	    
	if (r->dyn_state == DYN_DONE)
	    return memcache_parse_req(r);
	
	state = r->dyn_state;
	b = STAILQ_LAST(&r->mhdr, mbuf, next);    

        dmsg = r->dmsg;
        if (dmsg == NULL) {
            r->dmsg = dmsg_get();
            dmsg = r->dmsg;          
        }
	
	for (p = r->pos; p < b->last; p++) {
		ch = *p;
		loga("dyn parser req: for : state %d", state);
		switch (state) {
		        loga("memcache parser: main switch:  state %d %d]", state, ch);
		        case DYN_START:
                            loga("DYN_START");
		            if (ch == ' ') {
		                break;
		            } else if (isdigit(ch)) {
                               num = ch - '0'; 
                               state = DYN_MAGIC_NUMBER;
                            } 
                     
                            break;

                        case DYN_MAGIC_NUMBER:
                            loga("DYN_MAGIC_NUMBER");
                            loga("num = %d", num);
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
                            loga("DYN_SPACES_BEFORE_MSG_ID");
                            if (ch == ' ') {
                                break;
                            } else if (isdigit(ch)) {
                               num = ch - '0'; 
                               state = DYN_MSG_ID;
                            }

                            break;                       
           
                        case DYN_MSG_ID:
                            loga("DYN_MSG_ID");
                            loga("num = %d", num);
                            if (isdigit(ch))  {
                                num = num*10 + (ch - '0'); 
                            } else {  
                                if (num > 0) {
                                   loga("MSG ID : %d", num);
                                   dmsg->id = num;
                                   state = DYN_SPACES_BEFORE_TYPE_ID;
                                } else {
                                   goto error;
                                }
                            }
                            break;                         
                      
                        case DYN_SPACES_BEFORE_TYPE_ID:
                            loga("DYN_SPACES_BEFORE_TYPE_ID");
                            if (ch == ' ') {
                                break;
                            } else if (isdigit(ch)) {
                               num = ch - '0'; 
                               state = DYN_TYPE_ID;
                            }

                            break;

                        case DYN_TYPE_ID:
                            loga("DYN_TYPE_ID");
                            loga("num = %d", num);
                            if (isdigit(ch))  {
                                num = num*10 + (ch - '0');
                            } else {
                                if (num > 0)  {
                                   loga("VERB ID: %d", num);
                                   dmsg->type = num;
                                   state = DYN_SPACES_BEFORE_VERSION;
                                } else {
                                   goto error;       
                                }
                            }

                            break;

                        case DYN_SPACES_BEFORE_VERSION:
                            loga("DYN_SPACES_BEFORE_VERSION");
                            if (ch == ' ') {
                                break;
                            } else if (isdigit(ch)) {
                               num = ch - '0';
                               state = DYN_VERSION;
                            }
                            break;

                        case DYN_VERSION:
                           loga("DYN_VERSION");
                           loga("num = %d", num);
                           if (isdigit(ch))  {
                                num = num*10 + (ch - '0');
                            } else {
                                if (ch == CR)  {
                                   loga("VERSION : %d", num);
                                   dmsg->version = num;
                                   state = DYN_CRLF_BEFORE_STAR;
                                } else {
                                   goto error;
                                }
                            }

                            break;
       
                        case DYN_CRLF_BEFORE_STAR:
                            loga("DYN_CRLF_BEFORE_STAR");
                            if (ch == LF)  {
                               state = DYN_STAR;
                            } else {
                               goto error;
                            }          
 
                            break;

                        case DYN_STAR:
                           loga("DYN_STAR");
                           if (ch = '*') {
                               state = DYN_DATA_LEN;
                               num = 0;
                           } else {
                               goto error;
                           }

                           break;

                        case DYN_DATA_LEN:
                           loga("DYN_DATA_LEN");
                           loga("num = %d", num);
                           if (isdigit(ch))  {
                                num = num*10 + (ch - '0');
                           } else {
                               if (ch == ' ')  {
                                  loga("Data len: %d", num);
                                  dmsg->mlen = num;
                                  state = DYN_SPACE_BEFORE_DATA;
                                  num = 0;
                               } else {
                                  goto error;
                               }
                           }
                           break;

                        case DYN_SPACE_BEFORE_DATA:
                           loga("DYN_SPACE_BEFORE_DATA");
                           state = DYN_DATA;
                           break;

                        case DYN_DATA:
                           loga("DYN_DATA");
                           p -= 1;
                           if (dmsg->mlen >= 0)  {
                               dmsg->data = p;
                               p += dmsg->mlen - 1;                  
                               state = DYN_CRLF_BEFORE_DONE;
                           } else {
                               goto error;
                           }
   
                           break;
                        
                        case DYN_CRLF_BEFORE_DONE:
                           loga("DYN_CRLF_BEFORE_DONE");
          
                           if (ch == CR)  {
                              p += 1;
                              if (*p == LF) {
                                 state = DYN_DONE;
                              } else {
                                 goto error;
                              }
                           } else {
                               goto error;
                           }
 
                           break;

                        case DYN_DONE:
                           loga("DYN_DONE");
                           r->pos = p;
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
       loga("at done with p at %d", p);
       dmsg_dump(r->dmsg); 
       log_hexdump(LOG_VERB, b->pos, mbuf_length(b), "dyn: parsed rsp %"PRIu64" res %d "
	                    "type %d state %d rpos %d of %d", r->id, r->result, r->type,
	                    r->dyn_state, r->pos - b->pos, b->last - b->pos);
       return memcache_parse_req(r);
	    
    error:
       loga("at error");
       r->result = DMSG_PARSE_ERROR;
       r->state = state;
       errno = EINVAL;

       log_hexdump(LOG_INFO, b->pos, mbuf_length(b), "parsed bad req %"PRIu64" "
                "res %d type %d state %d", r->id, r->result, r->type,
                r->state);
       return;

    return memcache_parse_req(r);  //fix me
}


void
dmsg_free(struct dmsg *dmsg)
{
    ASSERT(STAILQ_EMPTY(&dmsg->mhdr));

    log_debug(LOG_VVERB, "free dmsg %p id %"PRIu64"", dmsg, dmsg->id);
    nc_free(dmsg);
}


void
dmsg_put(struct dmsg *dmsg)
{
    log_debug(LOG_VVERB, "put dmsg %p id %"PRIu64"", dmsg, dmsg->id);

    while (!STAILQ_EMPTY(&dmsg->mhdr)) {
        struct mbuf *mbuf = STAILQ_FIRST(&dmsg->mhdr);
        mbuf_remove(&dmsg->mhdr, mbuf);
        mbuf_put(mbuf);
    }

    nfree_dmsgq++;
    TAILQ_INSERT_HEAD(&free_dmsgq, dmsg, m_tqe);
}

void
dmsg_dump(struct dmsg *dmsg)
{
    struct mbuf *mbuf;

    loga("dmsg dump: id %"PRIu64" version %d type %d len %"PRIu32"  ", dmsg->id, dmsg->version, dmsg->type, dmsg->mlen);

    STAILQ_FOREACH(mbuf, &dmsg->mhdr, next) {
        uint8_t *p, *q;
        long int len;

        p = mbuf->start;
        q = mbuf->last;
        len = q - p;

        loga_hexdump(p, len, "mbuf with %ld bytes of data", len);
    }
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

    dmsg = nc_alloc(sizeof(*dmsg));
    if (dmsg == NULL) {
        return NULL;
    }

done:
    dmsg->id = ++dmsg_id;

    STAILQ_INIT(&dmsg->mhdr);
    dmsg->mlen = 0;
    dmsg->data = NULL;

    dmsg->type = MSG_UNKNOWN;
    dmsg->id = 0;
    dmsg->version = VERSION_10;
    

    return dmsg;
}


rstatus_t 
dmsg_write(struct mbuf *mbuf, uint64_t msg_id, uint8_t type, uint8_t version, struct string *data)
{

    mbuf_write_string(mbuf, &MAGIC_STR);
    mbuf_write_uint64(mbuf, msg_id);
    mbuf_write_char(mbuf, ' ');
    mbuf_write_uint8(mbuf, type);
    mbuf_write_char(mbuf, ' ');
    mbuf_write_uint8(mbuf, version);
    mbuf_write_string(mbuf, &CRLF_STR);
    mbuf_write_char(mbuf, '*');
    mbuf_write_uint32(mbuf, data->len);
    mbuf_write_char(mbuf, ' ');
    mbuf_write_string(mbuf, data);
    mbuf_write_string(mbuf, &CRLF_STR);

    log_hexdump(LOG_VERB, mbuf->pos, mbuf_length(mbuf), "dyn message ");
     
    return NC_OK;
}


rstatus_t 
dmsg_process(struct context *ctx, struct conn *conn, struct dmsg *dmsg)
{
    ASSERT(dmsg != NULL);
    ASSERT(conn->dyn_mode);

    struct string s;

    loga("dmsg process: type %d", dmsg->type);
    switch(dmsg->type) {
        case DMSG_DEBUG:
           s.len = dmsg->mlen;
           s.data = dmsg->data;
           log_hexdump(LOG_VERB, s.data, s.len, "dyn processing message ");
           break;
        case GOSSIP_DIGEST_SYN:
           break;

        case GOSSIP_DIGEST_ACK:
          break;

        case GOSSIP_DIGEST_ACK2:
          break;
        default:
          loga("nothing to do");
    }
       
    return NC_OK;
}


