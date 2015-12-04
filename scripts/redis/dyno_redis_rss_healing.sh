#!/bin/bash
# The following script checks the available memory and the Redis fragmentation ratio.
# If both are above a specific value, then it triggers Redis background rewrite AOF.
# Then restarts Dynomite. This eventually decreases the RSS ratio = 1.

# author: Ioannis Papapanagiotou

# To be used for exit code
declare -i RESULT

# Function to handle arguments
function usage () {
    cat <<EOF
Usage: $0 [options] [--] [file...]

Arguments:

  -h, --help
    Display this usage message and exit.

  -f, -- free_memory
    set the threshold for available node memory in KB

  -r, --redis_rss
    set the threshold for Redis RSS fragmentation

EOF
}

# handy logging and error handling functions
function log() { printf '%s\n' "$*"; }
function error() { log "ERROR: $*" >&2; }
function fatal() { error "$*"; exit 1; }
function usage_fatal() { error "$*"; usage >&2; exit 1; }


function restart_redis(){
   # take the pid of the redis-server.
   pid=`ps -ef | grep  'redis-server' | awk ' {print $2}'`

   # check number of Redis jobs
   RUNNING_REDIS=`ps -ef | grep  'redis-server' | grep 22122 | awk ' {print $2}' | wc -l`
   if [[ ${RUNNING_REDIS} -eq 1 ]]; then
      log "OK: killing redis"
      kill -9 $pid

      # check if Redis is still running after killing it
      REDIS_KILLED=`ps aux | grep redis-server | grep 22122 | wc -l`
      if [[ ${REDIS_KILLED} -eq 0 ]]; then
           log "OK: redis killed - sleeping 1 second"
           sleep 2
           log "OK: relaunching redis - sleeping 10 seconds"
           redis-server --port 22122 &
           sleep 10

           # check if Redis running after relauncing it"
           REDIS_RESTARTED=`ps aux | grep redis-server | grep 22122 | wc -l`
           if [[ ${REDIS_RESTARTED} -eq 1 ]]; then
               log "OK: redis launched - sleeping 20 seconds"
               log "==================================================="
               sleep 20
               else
                   log "ERROR: redis could not be relaunched"
                   ((RESULT++))
                   quit $RESULT
           fi
         else
             log "ERROR: redis could not be killed"
             log "ERROR: process running: `ps aux | grep redis-server | grep 22122`"
             ((RESULT++))
             quit $RESULT
         fi
    else
      ((RESULT++))
      log "ERROR: $RUNNING_REDIS redis-servers running. Exiting ..."
    fi
}


# Default values for threshold for available node memory in KB
FREE_MIN_MEMORY=5000000
# Default value for threshold for Redis RSS framgentation
THRESHOLD_REDIS_RSS=1.5

parser=0;

# parse options
while [ "$#" -gt 0 ]; do
    arg=$1
    case $1 in
        -f|--free_memory) shift;
           FREE_MIN_MEMORY=$1; ((parser++));;
        -r|--redis_rss) shift; THRESHOLD_REDIS_RSS=$1; ((parser++));;
        -h|--help) usage; exit 0;;
        -*) usage_fatal "unknown option: '$1'";;
        *) break;; # reached the list of file names
    esac
    shift || usage_fatal "option '${arg}' requires a value"
done

if [[ ${parser} -eq 0 ]]; then
   log "INFO: using the default values - memory threshold: 5GB and redis RSS ratio: 1.5"
elif [[ ${parser} -eq 1 ]]; then
   log "INFO: memory threshold set to: $FREE_MIN_MEMORY KB and redis RSS ratio set to: $THRESHOLD_REDIS_RSS"
else
   log "INFO: memory threshold set to: $FREE_MIN_MEMORY KB and redis RSS ratio default value: $THRESHOLD_REDIS_RSS"
fi

REDIS_UP=`redis-cli -p 22122 ping | grep -c PONG`
if [[ ${REDIS_UP} -ne 1 ]]; then
    ((RESULT++))
    log "INFO: REDIS is not running"
    exit $RESULT
fi

# Determine the available memory
FREE_MEMORY=`cat /proc/meminfo | sed -n 2p | awk -F ':        ' '{print $2}' | awk -F ' kB' '{print $1}'`
log "OK: Free memory in MB:  $(($FREE_MEMORY/1024)) "
# Check if available < 5GB
if [[ ${FREE_MEMORY} -le ${FREE_MIN_MEMORY} ]]; then

     # Determine the Redis RSS fragmentation ratio
     REDIS_RSS_FRAG=`redis-cli -p 22122 info | grep mem_fragmentation_ratio | awk -F ':' '{printf "%.2f\n",$2}'`
     log "OK: Redis RSS fragmentation: $REDIS_RSS_FRAG"

     # check if fragmentation is above threshold.
     # note the >, this is because we compare strings - bash does not support floating numbers.
      if (( $(echo "scale=2; $REDIS_RSS_FRAG > $THRESHOLD_REDIS_RSS;" | bc -l) )); then
         log "OK: Redis BGREWRITEAOF starting"
         REWRITEAOF=`redis-cli -p 22122 BGREWRITEAOF | grep ERR | wc -l`

         # check if BGREWRITEAOF can be competed successfully; otherwise fall back to synchronous SAVE.
         if (( $REWRITEAOF != 0)); then
            log "INFO: Redis BGREWRITEAOF failed - falling back to synchronous SAVE"
            log "OK: Starting Redis SAVE - Note: client is stalled during that process"
            redis-cli -p 22122 save

            log "OK: sleeping for 10 seconds post Redis SAVE"
            sleep 10            
            restart_redis

            # check if BGREWRITEAOF can be completed, after restarting Redis.
	    # eventually want to have an AOF file stored. According to Redis documantation,
  	    # if both AOF and RDB have been saved, AOF is preferred when starting Redis.
	    if [[ ${REDIS_UP} -ne 1 ]]; then
		((RESULT++))
         	log "INFO: REDIS is not running"
		quit $RESULT
	    fi

            if [[ $REWRITEAOF -ne 0]]; then
                log "ERROR: Redis BGREWRITEAOF failed again - post restarting Redis"
                ((RESULT++))
                quit $RESULT
            fi
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
         restart_redis
         log "==================================================="
     else
       log "INFO: Redis RSS fragmentation is $REDIS_RSS_RAM < $THRESHOLD_REDIS_RSS . Exiting..."
     fi
else
    if [[ ${FREE_MIN_MEMORY} -le 1024 ]]; then
       log "INFO: Available memory is $FREE_MEMORY KB more than $FREE_MIN_MEMORY KB. Exiting..."
    else
       log "INFO: Available memory is $(($FREE_MEMORY/1024)) MB more than $(($FREE_MIN_MEMORY/1024)) MB. Exiting..."
    fi
fi
exit $RESULT
        



