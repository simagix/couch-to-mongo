# CouchDB to MongoDB Migration

Migrate data from CouchDB to MongoDB.

## Steps

- Set up CouchDB ([2.3.1 macos](https://dl.bintray.com/apache/couchdb/mac/2.3.1/Apache-CouchDB-2.3.1.zip))
- Run `scripts/seed.sh` to populate data to CouchDB
- Spin up a MongoDB replica set (replset) and create a user:password login
    - or `mlaunch init --dir ~/data --replicaset --auth`
- Run `gradle --quiet run`

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
