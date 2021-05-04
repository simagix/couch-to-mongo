#!/bin/bash
docker rmi -f simagix/couch-to-mongo
id=$(docker create simagix/couch-to-mongo)
docker cp $id:/dist - | tar vx
docker rm $id
