#!/bin/sh

basepath='/gmservices/DATAOPS/SUPPRESSION_REQUEST'
PYTHON_PATH='/home/zxdev/lptServices/PYTHON_FILES/'
pidPath=$basepath/pidfile.txt

if [ ! -f $pidPath ]; then
  echo "PID created" >$pidPath
  echo "Suppression Request Picker Service Started :: $(date)"

  while true; do

    export pythonPath=$PYTHON_PATH/site-packages
    $PYTHON_PATH/python37/bin/python3.7 $basepath/suppressionRequestScheduler.py
    sleep 20
  done

  rm $pidPath

  echo "Suppression Request Picker service Execution Ended :: $(date)"
else
  echo "Suppression Request Picker service seems to be already in-progress, please verify. If not, the pid file might got stuck, pid file : ${pidPath} "
fi
