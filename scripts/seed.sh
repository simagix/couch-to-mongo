#! /bin/bash
db="demo"

if [ "$1" != "" ]; then
    db=$1
fi

curl -H 'Content-Type: application/json' \
    -X DELETE http://127.0.0.1:5984/$db
curl -H 'Content-Type: application/json' \
    -X PUT http://127.0.0.1:5984/$db

id=10001
for i in {1001..1250}
do
    for j in {1..20}
    do
        value=$(expr $i + $j)
        str=$(date)
        curl -X PUT http://127.0.0.1:5984/$db/"${id}" -d \
            "{ \"value\": ${value}, \"ts\": \"$str\" }"
        id=$(expr $id + 1)
    done
done