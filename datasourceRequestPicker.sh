#!/bin/sh

basepath='/gmservices/DATAOPS/DATASET'
PYTHON_PATH='/gmservices/DATAOPS/PYTHON_FILES/'
pidPath=$basepath/pidfile.txt

if [ ! -f $pidPath ];then
echo "PID in Progress" > $pidPath
echo "Data Source Request Picker Service Started :: `date`"

while true ; do

export PYTHONPATH=$PYTHON_PATH/site-packages
$PYTHON_PATH/python37/bin/python3.7 $basepath/datasourceRequestScheduler.py
sleep 20
done
if [ $? -eq 0 ]; then
        echo "Request Picker service Ended :: `date`"
fi

rm $pidPath

echo "Request Picker service Execution Ended :: `date`"
fi
