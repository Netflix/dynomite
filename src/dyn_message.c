#include <ctype.h>

#include <nc_core.h>
#include <nc_proto.h>

#include <dyn_message.h>



static uint64_t dmsg_id;          /* message id counter */
static uint32_t nfree_dmsgq;      /* # free msg q */
static struct dmsg_tqh free_dmsgq; /* free msg q */

static uint32_t MAGIC_NUMBER = 2014;


void
dyn_parse_req(struct msg *r)
{
	struct mbuf *b;
	uint8_t *p, *m;
	uint8_t ch;
        uint32_t num = 0;
        uint8_t iteration = 0;
        uint8_t num_args = 0; 
        uint32_t num_bytes = 0;

	enum {
		  DYN_START,
		  DYN_MAGIC_NUMBER = 1000,
                  DYN_SPACES_BEFORE_MSG_ID,
                  DYN_MSG_ID,
                  DYN_SPACES_BEFORE_VERB_ID,
                  DYN_VERB_ID,
                  DYN_SPACES_BEFORE_VERSION,
                  DYN_VERSION,
                  DYN_CRLF_BEFORE_STAR,
                  DYN_STAR,
                  DYN_NUM_ARGS,
                  DYN_CRLF_BEFORE_DOLLAR_SIGN,
                  DYN_DOLLAR_SIGN,
                  DYN_NUM_BYTES,
                  DYN_CRLF_BEFORE_DATA,
                  DYN_DATA,
                  DYN_CRLF_END_DATA,
		  DYN_DONE
	} state;
	    
	if (r->dyn_state == DYN_DONE)
		return memcache_parse_req(r);
	
	state = r->dyn_state;
	b = STAILQ_LAST(&r->mhdr, mbuf, next);    
	
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
                                   num = 0;
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
                                   state = DYN_SPACES_BEFORE_VERB_ID;
                                   num = 0;
                                } else {
                                   goto error;
                                }
                            }
                            break;                         
                      
                        case DYN_SPACES_BEFORE_VERB_ID:
                            loga("DYN_SPACES_BEFORE_VERB_ID");
                            if (ch == ' ') {
                                break;
                            } else if (isdigit(ch)) {
                               num = ch - '0'; 
                               state = DYN_VERB_ID;
                            }

                            break;

                        case DYN_VERB_ID:
                            loga("DYN_VERB_ID");
                            loga("num = %d", num);
                            if (isdigit(ch))  {
                                num = num*10 + (ch - '0');
                            } else {
                                if (num > 0)  {
                                   loga("VERB ID: %d", num);
                                   state = DYN_SPACES_BEFORE_VERSION;
                                   num = 0;
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
                                   state = DYN_CRLF_BEFORE_STAR;
                                   num = 0;
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
                               state = DYN_NUM_ARGS;
                               num = 0;
                           } else {
                               goto error;
                           }

                           break;

                        case DYN_NUM_ARGS:
                           loga("DYN_NUM_ARGS");
                           loga("num = %d", num);
                           if (isdigit(ch))  {
                                num = num*10 + (ch - '0');
                           } else {
                               if (ch == CR)  {
                                  loga("Num args: %d", num);
                                  num_args = num;
                                  state = DYN_CRLF_BEFORE_DOLLAR_SIGN;
                                  num = 0;
                               } else {
                                  goto error;
                               }
                           }
                           break;

                        case DYN_CRLF_BEFORE_DOLLAR_SIGN:
                           loga("DYN_CRLF_BEFORE_DOLLAR_SIGN");
                           if (ch == LF)  {
                               state = DYN_DONE;
                               goto done;;
                            } else {
                               goto error;
                            }
   
                           break;
     /*
                        case DYN_DOLLAR_SIGN:
                           if (ch = '*')  {
                               state = DYN_NUM_BYTES;  
                           } else {
                               goto error;
                           }


                           break;
                      
                        case DYN_NUM_BYTES:
                           if (isdigit(ch))  {
                                num += iteration * (ch - '0');
                                iteration++;
                           } else {
                                if (num > 0)  {
                                    loga("Num bytes: %d", num);
                                    num_bytes = num;
                                    state = DYN_CRLF_BEFORE_DATA;
                                    num = 0;
                                    iteration = 0;
                                } else {
                                    goto error;
                                }
        
                           }
                       
                           break;
 
                        case DYN_CRLF_BEFORE_DATA:
                               p += num_bytes;
                               num_bytes = 0;
                               //store the value in somewhere
                               
                           break;

                        case 
  */
                        /*

		        case DYN_MAGIC_CHAR:
                            loga("DYN_MAGIC_CHAR");
		            if (ch == ' ') {
		        	   break;
		            }
		        	
		            if (isdigit(ch) && isdigit(*(p+1)) && isdigit(*(p+2)) ) {
		               r->verb_start = p;
                               p = p + 3;
		               state = DYN_SPACES_BEFORE_IP;
		            } else {
		               goto error;
		            }
		        	
		            break;
		            
		        case DYN_SPACES_BEFORE_IP:
                            loga("DYN_SPACES_BEFORE_IP");
		            if (ch == ' ') {
		                break;
		            }
		        	
		            state = DYN_IP;
		            break;
		        	
		        case DYN_IP:
                            loga("DYN_IP");
		            //fix me for checking
		            r->ip_start = p;
		            p = p + 8;
		            state = DYN_DONE;
		            goto done;
		            break;
                        */
		        default:
		            NOT_REACHED();
		            break;
		        	
		}
		
	}
	
	done:
            loga("at done with p at %d", p);

	    r->pos = p + 1;
	    r->dyn_state = DYN_DONE; 
            b->pos = p + 1; 
            	
    
	    log_hexdump(LOG_VERB, b->pos, mbuf_length(b), "dyn: parsed rsp %"PRIu64" res %d "
	                    "type %d state %d rpos %d of %d", r->id, r->result, r->type,
	                    r->dyn_state, r->pos - b->pos, b->last - b->pos);
	    return memcache_parse_req(r);
	    
    error:
            loga("at error");
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

    loga("dmsg dump id %"PRIu64"  len %"PRIu32" type %d  ", dmsg->id, dmsg->mlen, dmsg->type);

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
    /* c_tqe, s_tqe, and m_tqe are left uninitialized */
    dmsg->id = ++dmsg_id;


    STAILQ_INIT(&dmsg->mhdr);
    dmsg->mlen = 0;
    dmsg->state = 0;
    dmsg->pos = NULL;
    dmsg->token = NULL;

    dmsg->type = MSG_UNKNOWN;
        //dmsg->key_start = NULL;
    //dmsg->key_end = NULL;

    //dmsg->vlen = 0;
    //dmsg->end = NULL;


    //dmsg->narg_start = NULL;
    //dmsg->narg_end = NULL;
    //dmsg->narg = 0;
    //dmsg->rnarg = 0;
    //dmsg->rlen = 0;
    //dmsg->integer = 0;

    //dmsg->error = 0;

    return dmsg;
}



rstatus_t
dmsg_write(struct dmsg *dmsg)
{
    rstatus_t status;
    struct dmsg *nmsg;
    struct mbuf *mbuf;
    size_t msize;
    ssize_t n;

    mbuf = STAILQ_LAST(&dmsg->mhdr, mbuf, next);
    if (mbuf == NULL || mbuf_full(mbuf)) {
        mbuf = mbuf_get();
        if (mbuf == NULL) {
            return NC_ENOMEM;
        }
        mbuf_insert(&dmsg->mhdr, mbuf);
        dmsg->pos = mbuf->pos;
        loga("in here");
    }
    ASSERT(mbuf->end - mbuf->last > 0);

    msize = mbuf_size(mbuf);

    loga("msize : %d", msize);
    loga("last - start = %d", (mbuf->last - mbuf->start));

    struct string s = string("lovelyday");

    mbuf_copy(mbuf, s.data, s.len);

    loga("last - start = %d", (mbuf->last - mbuf->start));
    //ASSERT((mbuf->last + ) <= mbuf->end);
    //mbuf->last += s.len;
    dmsg->mlen += (uint32_t)s.len;

    loga("mbuf: start=%d, last=%d, pos=%d, end=%d", mbuf->start, mbuf->last, mbuf->pos, mbuf->end);
    return NC_OK;
}







