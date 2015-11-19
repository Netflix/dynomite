#!/bin/bash
# The following script checks the available memory and the Redis fragmentation ratio.
# If both are above a specific value, then it triggers Redis background rewrite AOF.
# Then restarts Dynomite. This eventually decreases the RSS ratio = 1.

# author: Ioannis Papapanagiotou


#Handle arguments
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

# Default values for threshold for available node memory in KB
THRESHOLD_MEMORY=5000000
# Default value for threshold for Redis RSS framgentation
THRESHOLD_REDIS_RSS=1.5

parser=0;

# parse options
while [ "$#" -gt 0 ]; do
    arg=$1
    case $1 in
        -f|--free_memory) shift;
           THRESHOLD_MEMORY=$1; ((parser++));;
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
   log "INFO: memory threshold set to: $THRESHOLD_MEMORY KB and redis RSS ratio set to: $THRESHOLD_REDIS_RSS"
else
   log "INFO: memory threshold set to: $THRESHOLD_MEMORY KB and redis RSS ratio default value: $THRESHOLD_REDIS_RSS"
fi


declare -i RESULT

REDIS_UP=`redis-cli -p 22122 ping | grep -c PONG`
if [[ ${REDIS_UP} -ne 1 ]]; then
    ((RESULT++))
    log "INFO: REDIS is not running"
    exit $RESULT
fi

# Determine the available memory
FREE_MEMORY=`cat /proc/meminfo | sed -n 2p | awk -F ':        ' '{print $2}' | awk -F ' kB' '{print $1}'`
log "OK: Free memory in MB:  $(($FREE_MEMORY/1000)) "

# Check if available < 5GB
if [[ ${FREE_MEMORY} -le ${THRESHOLD_MEMORY} ]]; then

     # Determine the Redis RSS fragmentation ratio
     REDIS_RSS_FRAG=`redis-cli -p 22122 info | grep mem_fragmentation_ratio | awk -F ':' '{print $2}'`
     log "OK: Redis RSS fragmentation: $REDIS_RSS_FLAG"

     # check if fragmentation is above threshold.
     # note the >, this is because we compare strings - bash does not support floating numbers
     if [[ ${REDIS_RSS_FRAG} > ${THRESHOLD_REDIS_RSS} ]]; then
          log "OK: bgrewrite aof starting"
          redis-cli -p 22122 BGREWRITEAOF
          log "OK: bgrewriteaof completed - sleeping 2 seconds"            
          sleep 2

          pid=`ps -ef | grep  'redis-server' | awk ' {print $2}'`

 	  # check number of Redis jobs
          RUNNING_REDIS=`ps -ef | grep  'redis-server' | grep 22122 | awk ' {print $2}' | wc -l`
          if [[ ${RUNNING_REDIS} -eq 1 ]]; then
		  kill -9 $pid
	          log "OK: killing redis"
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
        	     fi
        	 else
        	     log "ERROR: redis could not be killed"
        	     log "ERROR: process running: `ps aux | grep redis-server | grep 22122`"
        	     ((RESULT++))
        	 fi
          else
        	 ((RESULT++))
 		 log "ERROR: $RUNNING_REDIS redis-servers running. Exiting ..."
	  fi
     else
       log "INFO: Redis RSS fragmentation is $REDIS_RSS_RAM < $THRESHOLD_REDIS_RSS . Exiting..."
     fi
else
    log "INFO: Available memory is $(($FREE_MEMORY/1000))  more than $(($THRESHOLD_MEMORY/1000)) KB. Exiting..."
fi
exit $RESULT
        



