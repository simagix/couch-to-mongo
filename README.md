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

It should return 5000.