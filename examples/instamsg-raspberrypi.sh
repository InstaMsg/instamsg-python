#!/bin/sh
#
#
#
# chkconfig: 98 02
# description: This is a daemon for automatically
# start my service
#
# processname: instamsg
#
#

prefix=/usr
exec_prefix=/usr
sbindir=/usr/sbin
USER=pi
SCRIPT_LOCATION=/home/pi/Desktop/instamsg-example.py
SCRIPT_LOG_LOCATION=/var/log/instamsg
START_UP_LOG=${SCRIPT_LOG_LOCATION}/instamsg.log
SHUT_DOWN_LOG=${SCRIPT_LOG_LOCATION}/instamsg.log
INSTAMSG_PID_FILE=/var/run/instamsg.pid
PROCESS_NAME=instamsg
# Sanity checks.

# so we can rearrange this easily

RETVAL=0

start() {
  echo "Starting instamsg... "
  echo " log location is:" ${START_UP_LOG}
 if [ ! -f $TART_UP_LOG ]; then
    touch $TART_UP_LOG
  fi
  python $SCRIPT_LOCATION > $START_UP_LOG &
  if [ ! -f $INSTAMSG_PID_FILE ]; then
    touch $INSTAMSG_PID_FILE
  fi
  sleep 5 
  echo $(pgrep -f $PROCESS_NAME) > $INSTAMSG_PID_FILE
  echo "Done starting instamsg! "
}

stop() {
  echo "Stopping instamsg... "
  echo " log location is:" ${SHUT_DOWN_LOG}
  echo $(pgrep -f $PROCESS_NAME) > $INSTAMSG_PID_FILE
  echo $(cat $INSTAMSG_PID_FILE)
  kill -KILL $(cat $INSTAMSG_PID_FILE)
  rm -rf $INSTAMSG_PID_FILE
  echo "Done stopping instamsg! "
}

case "$1" in
start)
  if [ ! -f $START_UP_LOG ]; then
    touch $START_UP_LOG
  fi
  start
  ;;
stop)
  if [ ! -f $SHUT_DOWN_LOG ]; then
    touch $SHUT_DOWN_LOG
  fi
  stop
  ;;
status)
  if [ -f $INSTAMSG_PID_FILE ]; then
    echo "instamsg running, everything is fine."
  fi
  ;;
restart)
  stop
  start
  ;;
condrestart)
  if [ -f /var/lock/subsys/$servicename ]; then
  stop
  start
  fi
  ;;
*)
  echo $"Usage: $0 {start|stop|status|restart|condrestart}"
  ;;

esac
exit $RETVAL
