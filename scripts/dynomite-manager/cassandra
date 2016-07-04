#!/bin/bash
# chkconfig: 2345 95 20
# description: This script does some stuff
# processname: java

start() {
   echo "Starting cassandra..."
   export JAVA_HOME=/home/ec2-user/jdk1.8.0_45
   export JRE_HOME=/home/ec2-user/jdk1.8.0_45/jre
   export PATH=$PATH:/home/ec2-user/jdk1.8.0_45/bin:/home/ec2-user/jdk1.8.0_45/jre/bin

   cd /home/ec2-user/apache-cassandra-2.1.14
   bin/cassandra start & 
}

stop() {
   echo "stop"
   PID=`ps aux | grep cassandra | grep -v grep | awk '{print $2}'`
   if [[ "" !=  "$PID" ]]; then
      echo "killing $PID"
      sudo kill -9 $PID
   fi
}

case "$1" in start)
  start
;;
  stop)
  stop
;;
*)

echo $"Usage: $0 {start|stop}"
RETVAL=1
esac
exit 0



