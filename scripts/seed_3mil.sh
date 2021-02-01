#! /bin/bash
db="demo3mil"

if [ "$1" != "" ]; then
    db=$1
fi

curl -H 'Content-Type: application/json' \
    -X DELETE http://user:password@127.0.0.1:5984/$db
curl -H 'Content-Type: application/json' \
    -X PUT http://user:password@127.0.0.1:5984/$db

blah='"blah": "blah"'
for i in {6000..3001}
do
    counter=$(expr 6001 - $i)
    echo "seeding batch ${counter}/3000"
    docs='{"docs": ['
    id=${i}
    value=$i
    str=$(date)
    mesg='{ "_id": "'"${id}"'","value":'"${value}"',"ts": "'"${str}"'" '
    docs=$docs${mesg}'},'
    for j in {1100..101}
    do
        id=${i}${j}
        value=$(expr $i + $j)
        str=$(date)
        mesg='{ "_id": "'"${id}"'","value":'"${value}"',"ts": "'"${str}"'" '
        docs=$docs${mesg}','"${blah}"'},'
    done
    docs=${docs::${#docs}-1}']}'
    curl -X POST -H "Content-Type: application/json" http://user:password@127.0.0.1:5984/$db/_bulk_docs -d "${docs}" > /dev/null 2>&1 &
    sleep 1
done

for job in `jobs -p`
do
    wait $job
done