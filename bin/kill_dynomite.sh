#!/bin/bash

cmd="pkill -f /apps/dynomite/bin/dynomite"
if [ $USER != "root" ];then
    exec $cmd
else
    su  $userowner -c "$cmd"
fi

