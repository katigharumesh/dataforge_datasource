#!/bin/sh

basepath='/home/zxdev/zxcustom/DATAOPS/SUPPRESSION_REQUEST'
PYTHON_PATH='/home/zxdev/lptServices/PYTHON_FILES/'
pidPath=$basepath/pidfile.txt

if [ ! -f $pidPath ];then
echo "PID in Progress" > $pidPath
echo "Suppression Request Picker Service Started :: `date`"

while true ; do

export pythonPath=$PYTHON_PATH/site-packages
$PYTHON_PATH/python37/bin/python3.7 $basepath/suppressionRequestScheduler.py
sleep 20
done
if [ $? -eq 0 ]; then
        echo "Request Picker service Ended :: `date`"
fi

rm $pidPath

echo "Request Picker service Execution Ended :: `date`"
fi
