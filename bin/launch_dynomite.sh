#!/bin/bash

DYN_DIR=/apps/dynomite
LOG_DIR=/logs/system/dynomite
CONF_DIR=$DYN_DIR/conf

# this code lifted shamelessly from Mike T.'s /etc/init.d/tomcat
# figure out the appropriate user that we should run by
typeset -x `stat --printf "userowner=%U\ngroupowner=%G\n" $0`
mkdir -p $LOG_DIR
chown -R $userowner:nac  $LOG_DIR


# note that we do not use 'su - username .... ' , because we want to keep the env settings that we have done so far
cmd="$DYN_DIR/bin/dynomite -c $CONF_DIR/dynomite.yml -d --output=$LOG_DIR/dynomite.log  > $LOG_DIR/dynomite_start-$(date +%Y%m%d_%H:%M:%S).out 2>&1 &"
if [ $USER != "root" ];then
    exec $cmd
else
    su  $userowner -c "$cmd"
fi

