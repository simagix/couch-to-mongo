#! /bin/bash
NUM_RUNS=10

if [ "$1" != "" ]; then
    NUM_RUNS=$1
fi

PID=$(ps -ef | grep java | head -n 1 | grep couch-to-mongo | awk '{ print $2 }')
#PID=$(ps -ef | grep java | head -n 1 | awk '{ print $2 }')
echo "PID is $PID"

echo "Collecting $NUM_RUNS thread dumps"
for i in {1..$NUM_RUNS}
do
    jstack -F -l $PID >> "stacktrace.$PID.$i.txt";
done

