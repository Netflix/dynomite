#!/bin/bash
# chkconfig: 2345 95 20
# description: This script does some stuff
# processname: java

export JAVA_HOME=/home/ec2-user/jdk1.8.0_45
export JRE_HOME=/home/ec2-user/jdk1.8.0_45/jre
export PATH=$PATH:/home/ec2-user/jdk1.8.0_45/bin:/home/ec2-user/jdk1.8.0_45/jre/bin

export ASG_NAME="asg_dynomite"
export EC2_REGION="us-west-2"
export AUTO_SCALE_GROUP="asg_dynomite"

start() {
   echo "Starting Dynomite Manager..."
   cd /home/ec2-user/dynomite-manager/dynomite-manager/
   /home/ec2-user/dynomite-manager/dynomite-manager/gradlew jettyRun > /logs/system/dynomite-manager/dynomite-manager.log & 
}

stop() {
   echo "stoping Dynomite Manager... "
   PID=`ps -ef | grep gradlew | awk '{print $2}' ORS=' ' | awk '{print $1}'`
   if [[ "" !=  "$PID" ]]; then
      echo "killing $PID"
      sudo kill -9 $PID
   fi
}

debug() {
   echo "Starting Dynomite Manager for DEBUG..."
   cd /home/ec2-user/dynomite-manager/dynomite-manager/
   export GRADLE_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,address=8000,server=y,suspend=n"
   ./gradlew jettyRun &   
}


case "$1" in
"start")
  start
;;
"debug")
  debug
;;
 "stop")
  stop
;;
*)

echo $"Usage: $0 {start|stop|debug}"
RETVAL=1
esac
exit 0



