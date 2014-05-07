#!/bin/bash

cmd="pkill dynomite"
if [ $USER != "root" ];then
    exec $cmd
else
    su  $userowner -c "$cmd"
fi

