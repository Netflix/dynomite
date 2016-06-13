#!/bin/bash


# ** This check has to be after the redis check above.
# Quit if Dynomite is already running, rather than
# throwing errors from this script.
if (ps -ef | grep  '[/]apps/dynomite/bin/dynomite'); then
    logger -s "Dynomite already running, no need to restart it again."
    exit 0
fi

DYN_DIR=/apps/dynomite
LOG_DIR=/logs/system/dynomite
CONF_DIR=$DYN_DIR/conf


declare -x `stat --printf "userowner=%U\ngroupowner=%G\n" "$0"`

if [ ! -d "$LOG_DIR" ]; then
    sudo mkdir -p $LOG_DIR
    sudo chown -R $userowner:$groupowner $LOG_DIR
fi

#save the previous log
if [ -e $LOG_DIR/dynomite.log ]; then
    mv $LOG_DIR/dynomite.log $LOG_DIR/dynomite-$(date +%Y%m%d_%H%M%S).log
fi

echo "MBUF_SIZE=$MBUF_SIZE"
if [ -z "$MBUF_SIZE" ]; then
    echo "MBUF_SIZE is empty. Use default value 16K"
    MBUF_SIZE=16384
fi

echo "ALLOC_MSGS=$ALLOC_MSGS"
if [ -z "$ALLOC_MSGS" ]; then
    #** Requires setting the EC2 Instance type as ENV variable
    # If Dynomite is used outside of AWS environment the 
    # following can be used as ideas on how much memory Dynomite
    # should take.

    # Message allocation based on the instance type
    # 2GB for Florida + 85% for Redis (rest available for OS)
    if [ "$EC2_INSTANCE_TYPE" == "r3.xlarge" ]; then
        # r3.xlarge: 30.5GB RAM (2.5GB available)
        ALLOC_MSGS=100000
    elif [ "$EC2_INSTANCE_TYPE" == "r3.2xlarge" ]; then
       # r3.2xlarge: 61GB RAM (7.15GB available)
       ALLOC_MSGS=300000
    elif [ "$EC2_INSTANCE_TYPE" == "r3.4xlarge" ]; then
       # r3.4xlarge: 122GB RAM (16.3GB available)
       ALLOC_MSGS=800000
    elif [ "$EC2_INSTANCE_TYPE" == "r3.8xlarge" ]; then
       # r3.8xlarge: 244GB RAM (34.19GB available)
       # Dynomite uper threshold is 1M
       ALLOC_MSGS=1000000
    fi
    echo "Instance Type: $EC2_INSTANCE_TYPE --> Allocated messages: $ALLOC_MSGS"
fi


# note that we do not use 'su - username .... ' , because we want to keep the env settings that we have done so far
cmd="$DYN_DIR/bin/dynomite -d -c $CONF_DIR/dynomite.yml -m$MBUF_SIZE -M$ALLOC_MSGS --output=$LOG_DIR/dynomite.log "


if [ $USER != "root" ];then
    exec $cmd
else
    su  $userowner -c "$cmd"
fi

sleep 1

sudo $DYN_DIR/bin/core_affinity.sh

