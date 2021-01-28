#!/bin/bash
docker rmi -f simagix/couch-to-mongo
id=$(docker create simagix/couch-to-mongo)
docker cp $id:/couch-to-mongo.jar .
docker rm $id
