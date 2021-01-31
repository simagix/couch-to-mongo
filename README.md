# CouchDB to MongoDB Migration

Migrate data from CouchDB to MongoDB.  The algoirthm is based on the fact that in CouchDB, documents are sorted by the document ID.  The application uses a thread to read data off CouchDB and kicks off multiple threads to save data to MongoDB.

## Algorithm

- Read document IDs from CouchDB
- Divide the documents reading by *couch_batch_size*
- Use `startDocId` and `endDocId` to read all dccuments within the range
- Submit the task to the created thread pool to save data to the mongo server

## Setup Env

- Set up CouchDB ([2.3.1 macos](https://dl.bintray.com/apache/couchdb/mac/2.3.1/Apache-CouchDB-2.3.1.zip))
- Run `scripts/seed.sh` to populate data to CouchDB
- Spin up a MongoDB replica set (replset) and create a user:password login
    - or `mlaunch init --dir ~/data --replicaset --auth`

## Test

Run:

```bash
gradle --quiet run
```

Or run:

```bash
gradle build
java -jar build/libs/couch-to-mongo-0.0.1.jar
```

The default properties file is *migration.properties*.  You can include the properties file name in the command line, for example:

```bash
java -jar couch-to-mongo-0.0.1.jar /path/my.properties
```


## Validation

```bash
mongo --quiet "mongodb://user:password@localhost/demo?replicaSet=replset&authSource=admin" --eval 'db.sample_docs.count()'
```

It should return 12500.

## Test Results

Below is a result of migrating 12,500 documents, a total of 344MB, from CouchDB to MongoDB, both running on the same computer.

|Tests| # of Threads| CouchDB Batch Size| MongoDB Batch Size| Time|
|---|--:|--:|--:|--:|
|Test 1|4|1,000|100|27 seconds|
Test 2|4|3,200|1,100|25 seconds|
Test 3|8|5,000|1,000|31 seconds|

## Download from Docker Hub

```bash
#!/bin/bash
docker rmi -f simagix/couch-to-mongo
id=$(docker create simagix/couch-to-mongo)
docker cp $id:/dist - | tar vx
docker rm $id
```

## What's Next

- Resumable: this requires the migration application to take a snapshot of the original states
- Watch change streams of CouchDB
