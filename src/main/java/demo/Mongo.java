// Copyright 2020 Kuei-chun Chen. All rights reserved.
package demo;

import com.mongodb.*;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteInsert;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.result.InsertManyResult;
import org.bson.BsonValue;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.print.Doc;
import java.util.*;

public class Mongo implements AutoCloseable {
    private Logger logger = LoggerFactory.getLogger(Mongo.class);
    private MongoClient mongoClient;

    private final String MIGRATION_COLLECTION_METADATA_NAME = "migration.metadata";

    private final int MAX_NUM_INSERT_ATTEMPTS = 10;
    private final int MAX_NUM_READ_ATTEMPTS = 10;

    public Mongo(String mongodbURI) {
        ConnectionString connectionString = new ConnectionString(mongodbURI);
        MongoClientSettings clientSettings = MongoClientSettings.builder().applyConnectionString(connectionString)
                .build();
        mongoClient = MongoClients.create(clientSettings);
    }

    @Override
    public void close() throws Exception {
        mongoClient.close();
    }

	public int saveToMongo(String dbName, String collectionName, List<Document> documents) {
        long startTime = System.currentTimeMillis();

        logger.info("Saving data to mongo");

		if (documents.isEmpty()) {
		    logger.trace("Result set provided is empty. Not inserting.");
			return 0;
        }

        MongoCollection<Document> collection = mongoClient.getDatabase(dbName).getCollection(collectionName);
        InsertManyOptions options = new InsertManyOptions();
        options.ordered(false);

        int resultSize = 0;
		int numInsertAttempts = 0;

		while (numInsertAttempts < MAX_NUM_INSERT_ATTEMPTS) {
            numInsertAttempts++;
            logger.trace(String.format("Making write attempt %d/%d", numInsertAttempts, MAX_NUM_INSERT_ATTEMPTS));
            try {
                String message = String.format("documents size %d", documents.size());
                logger.info(message);

                InsertManyResult res = collection.insertMany(documents, options);
                resultSize = res.getInsertedIds().size();
                logger.info(String.format("Successfully inserted %d documents", resultSize));

                if (resultSize > 0) {
                    insertMetaData(dbName, res);
                    break;
                }

            } catch (MongoWriteException ex) {

                logger.error("Encountered MongoWriteException: " + ex);
                logger.error(ex.getMessage());
                sleep(2000);
                continue;

            } catch (MongoBulkWriteException ex) {

                insertMetaData(dbName, ex.getWriteResult(), ex.getWriteErrors());

                logger.error("Encountered MongoBulkWriteException: " + ex);
                logger.error(ex.getMessage());
                sleep(2000);
                break;
            } catch (Exception ex) {

                logger.error("Encountered exception: " + ex);
                logger.error(ex.getMessage());
                break;
            }
        }

        // Record time
        long endTime = System.currentTimeMillis();
        logger.debug(String.format("saveToMongo() lasted %d mills", endTime - startTime));

        return resultSize;
    }
    
    public long countDocuments(String dbName, String collectionName) {
        long startTime = System.currentTimeMillis();

        // Get number of documents
        MongoCollection<Document> collection = mongoClient.getDatabase(dbName).getCollection(collectionName);

        long numDocs = 0;
        int numReadAttempts = 0;
        while (numReadAttempts < MAX_NUM_READ_ATTEMPTS) {
            numReadAttempts++;
            logger.trace(String.format("Making count attempt %d/%d", numReadAttempts, MAX_NUM_READ_ATTEMPTS));
            try {
                numDocs = collection.countDocuments();
                break;
            } catch (MongoException ex) {
                logger.error("Encountered MongoBulkWriteException: " + ex);
                logger.info(ex.getMessage());
                sleep(2000);
                continue;
            } catch (Exception ex) {

                logger.error("Encountered exception: " + ex);
                logger.info(ex.getMessage());
                break;
            }
        }

        // Record time
        long endTime = System.currentTimeMillis();
        logger.debug(String.format("countDocuments() lasted %d mills", endTime - startTime));

        return numDocs;
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            logger.error(String.format("Encountered exception when sleeping for %d milllis: " + ex, millis));
            logger.error(ex.getMessage());
        }
    }

    private void insertMetaData(String dbName, InsertManyResult result) {
        WriteConcern wc = new WriteConcern(0);
        MongoCollection<Document> collection = mongoClient.getDatabase(dbName)
                .getCollection(MIGRATION_COLLECTION_METADATA_NAME)
                .withWriteConcern(wc);

        Set<BsonValue> insertedIds = new HashSet<>();
        for (BsonValue value : result.getInsertedIds().values()) {
            insertedIds.add(value.asString());
        }

        Document metaDataDoc = new Document("time", new Date()).append("insertedIds", insertedIds);
        collection.insertOne(metaDataDoc);
    }

    private void insertMetaData(String dbName, BulkWriteResult result, Collection<BulkWriteError> errors) {
        WriteConcern wc = new WriteConcern(0);
        MongoCollection<Document> collection = mongoClient.getDatabase(dbName)
                                                            .getCollection(MIGRATION_COLLECTION_METADATA_NAME)
                                                            .withWriteConcern(wc);

        Set<BsonValue> insertedIds = new HashSet<>();
        for (BulkWriteInsert bulkWriteInsert : result.getInserts()) {
            insertedIds.add(bulkWriteInsert.getId());
        }

        Set<BsonValue> upsertedIds = new HashSet<>();
        for (BulkWriteUpsert bulkWriteUpsert : result.getUpserts()) {
            upsertedIds.add(bulkWriteUpsert.getId());
        }

        Document metaDataDoc = new Document("time", new Date()).append("insertedIds", insertedIds)
                                                                .append("upsertedIds", upsertedIds)
                                                                .append("error", errors);

        collection.insertOne(metaDataDoc);
    }

    // TODO : may not need this
    private Set<Document> getUninsertedDocuments(Set<Document> documents, InsertManyResult result) {

        Collection<BsonValue> insertedIds = result.getInsertedIds().values();
        Set<Document> newDocuments = new HashSet<>();
        for (Document document : documents ) {
            if (insertedIds.contains((BsonValue) document.get("_id"))) {
                continue;
            }
        }
        return newDocuments;
    }
}
