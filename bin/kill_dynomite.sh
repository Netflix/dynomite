#!/bin/bash

cmd="pkill /apps/dynomite/bin/dynomite"
if [ $USER != "root" ];then
    exec $cmd
else
    su  $userowner -c "$cmd"
fi

