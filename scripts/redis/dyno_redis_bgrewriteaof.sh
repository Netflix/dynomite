#!/bin/bash
# The following script checks runs Redis background rewrite AOF.

declare -i RESULT

# handy logging and error handling functions
function log() { printf '%s\n' "$*"; }

# Start by checking if Redis is running
REDIS_UP=`redis-cli -p 22122 ping | grep -c PONG`
if [[ ${REDIS_UP} -ne 1 ]]; then
    ((RESULT++))
    log "ERROR: Redis is not running"
    quit $RESULT
fi

log "OK: Redis BGREWRITEAOF starting"

# check if BGREWRITEAOF can be competed successfully.
REWRITEAOF=`redis-cli -p 22122 BGREWRITEAOF | grep ERR | wc -l`
if (( $REWRITEAOF != 0)); then
    ((RESULT++))
    log "ERROR: Redis BGREWRITEAOF failed"
    quit $RESULT
fi

SLEEPING=2
log "OK: sleeping initial $SLEEPING seconds post bgrewriteaof"
sleep $SLEEPING

# If bgrewriteaof is still running, we iterate inside a loop that waits for bg_rewrite_aof to finish.
# Exponential backoff adds 5 seconds to the sleeping time until the value aof_rewrite_in_progress is zero.
# If the sleep takes too long (1800 seconds = 30 min), the process quits.  
REDIS_AOF_REWRITE_IN_PROGRESS=`redis-cli -p 22122 INFO | grep aof_rewrite_in_progress | awk -F ':' '{printf "%d\n",$2}'`
while [[  ${REDIS_AOF_REWRITE_IN_PROGRESS} -gt 0 ]]; do
      sleep $SLEEPING
      log "OK: sleeping $SLEEPING because BGREWRITEAOF is pending"
      REDIS_AOF_REWRITE_IN_PROGRESS=`redis-cli -p 22122 INFO | grep aof_rewrite_in_progress | awk -F ':' '{printf "%d\n",$2}'`
      let SLEEPING=2*SLEEPING
      if [[ ${SLEEPING} -ge 1800 ]]; then
         log "ERROR: Redis BGREWRITEAOF takes more than 1800 seconds"
         ((RESULT++))
         quit $RESULT
      fi
done
log "==================================================="
