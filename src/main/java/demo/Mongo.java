// Copyright 2020 Kuei-chun Chen. All rights reserved.
package demo;

import com.mongodb.*;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteInsert;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.client.*;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.result.InsertManyResult;
import org.bson.BsonValue;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Mongo implements AutoCloseable {
    private Logger logger = LoggerFactory.getLogger(Mongo.class);
    private MongoClient mongoClient;
    private String dbName;

    private UUID sessionId;
    private Set<String> existingIds = null;

    private final String MIGRATION_COLLECTION_METADATA_NAME = "migration.metadata";
    private final int MAX_NUM_INSERT_ATTEMPTS = 10;
    private final int MAX_NUM_READ_ATTEMPTS = 10;

    public Mongo(String mongodbURI, String dbName) {
        ConnectionString connectionString = new ConnectionString(mongodbURI);
        MongoClientSettings clientSettings = MongoClientSettings.builder().applyConnectionString(connectionString)
                .build();
        mongoClient = MongoClients.create(clientSettings);
        this.dbName = dbName;

        UUID unfinishedSessionId = getUnfinishedSession(dbName);
        this.sessionId = unfinishedSessionId;
        if (null == unfinishedSessionId) {
            this.sessionId = UUID.randomUUID();
            logger.info("Starting session with id... " + this.sessionId);
            insertMetaDataOperation("start", new Date());

            // Create empty set of existing Ids, as we will not need to find existing documents in MongoDB, because this
            // is a new session, not a resumption of previously crashed session
            existingIds = new HashSet<>();
        } else {

            insertMetaDataOperation("resume", new Date());
            logger.info("Resuming session with id... " + this.sessionId);
        }
    }

    @Override
    public void close() throws Exception {
        mongoClient.close();
    }

    /***
     * Mongo Contains Id
     *
     * @param dbName
     * @param collectionName
     * @param id
     * @return
     */
    public boolean mongoContainsId(String dbName, String collectionName, String id) {
        // Lazily initialize
        if (existingIds == null) {
            logger.debug("Lazily initializing existing ids...");
            existingIds = getExistingIds(dbName, collectionName);
        }
        return existingIds.contains(id);
    }

	public int saveToMongo(String dbName, String collectionName, List<Document> documents, Long threadId) {
        long startTime = System.currentTimeMillis();

        logger.info("Saving data to mongo");

		if (documents.isEmpty()) {
		    logger.trace("Result set provided is empty. Not inserting.");
			return 0;
        }

		// Get document sequence numbers
        logger.info("Getting document sequence numbers");
		Map<String, String> docIdToSeqNum = new HashMap<>();
		for (Document document : documents) {
		    String id = (String) document.get("_id");
		    String documentSeqNum = (String) document.get("DocumentSequenceNumber");
		    if (null == documentSeqNum) {
		        documentSeqNum = "";
            }

		    logger.trace(String.format("Inserting document sequence number %s for id %s", documentSeqNum, id));
		    docIdToSeqNum.put(id, documentSeqNum );
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
                    insertMetaData(dbName, threadId, res, docIdToSeqNum);
                    break;
                }

            } catch (MongoWriteException ex) {

                logger.error("Encountered MongoWriteException: " + ex);
                logger.error(ex.getMessage());
                sleep(2000);
                continue;

            } catch (MongoBulkWriteException ex) {

                insertMetaData(dbName, threadId, ex.getWriteResult(), ex.getWriteErrors(), docIdToSeqNum);

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

    /****
     * Insert Meta Data Message
     *
     * Inserts a
     *
     * @param logMessage        A String representing the message to log
     * @param date              A Date object representing the timestamp of the log message
     */
    public void insertMetaDataOperation(String logMessage, Date date) {

        // WC = 0 is effectively an asynchronous write. Metadata ops should be async to maintain performance
        WriteConcern wc = new WriteConcern(0);
        MongoCollection<Document> collection = mongoClient.getDatabase(dbName)
                                                            .getCollection(MIGRATION_COLLECTION_METADATA_NAME)
                                                            .withWriteConcern(wc);

        Document startTimeDoc = new Document("operation",  logMessage).append("time", date).append("session", sessionId);
        collection.insertOne(startTimeDoc);
    }

    /***
     * Get Unfinished Session
     *
     * @param dbName
     * @return
     */
    private UUID getUnfinishedSession(String dbName) {
        WriteConcern wc = new WriteConcern(0);
        MongoCollection<Document> collection = mongoClient.getDatabase(dbName)
                .getCollection(MIGRATION_COLLECTION_METADATA_NAME)
                .withWriteConcern(wc);

        // Get existing session data
        List<Document> getSessionsPipelineStages = new ArrayList<>();

        List<String> startEndOps = new ArrayList<>();
        startEndOps.add("start");
        startEndOps.add("end");
        Document matchStage = new Document("$match", new Document("operation", new Document("$in", startEndOps)));
        getSessionsPipelineStages.add(matchStage);

        // { operation : "start" } will appear before { operation : "last" }
        Document sortByOpTypeStage = new Document("$sort", new Document("operation", -1));
        getSessionsPipelineStages.add(sortByOpTypeStage);

        Document groupStage = new Document("$group", new Document("_id", "$sessionId").append("start", new Document("$first", "$start"))
                                                                                        .append("end", new Document("$first", "$end"))
                                                                                        .append("startTime", new Document("$first", "$time"))
                                                                                        .append("endTime", new Document("$last", "$time")));
        getSessionsPipelineStages.add(groupStage);

        Document sortByStartTimeInDescendingOrderStage = new Document("$sort", new Document("$startTime", -1));
        getSessionsPipelineStages.add(sortByStartTimeInDescendingOrderStage);

        Document limitToOneSessionStage = new Document("$limit", 1);
        getSessionsPipelineStages.add(limitToOneSessionStage);

        AggregateIterable<Document> sessionsData = collection.aggregate(getSessionsPipelineStages);
        for (Document sessionDataDocument : sessionsData) {

            // If there is no end document, then we know that this session was interrupted
            if (sessionDataDocument.get("end") == null) {
                return (UUID) sessionDataDocument.get("sessionId");
            }
            break;
        }

        // Returning null indicates that the previous session completed
        return null;
    }

    /***
     * Get Existing Ids
     *
     * @param dbName
     * @param collectionName
     * @return
     */
    private Set<String> getExistingIds(String dbName, String collectionName) {
        long startTime = System.currentTimeMillis();

        logger.info(String.format("Getting existing ids in MongoDB namespace %s.%s", dbName, collectionName));
        Set<String> ids = new HashSet<>();

        MongoCollection<Document> collection = mongoClient.getDatabase(dbName).getCollection(collectionName);
        Document queryAllDoc = new Document();
        Document projectionOnlyIdDocument = new Document("_id", 1);
        Document sortOnIdDoc = new Document("_id", 1);

        FindIterable<Document> cursor = collection.find(queryAllDoc).projection(projectionOnlyIdDocument).sort(sortOnIdDoc);
        for (Document document : cursor) {
            logger.trace("Adding " + document.get("_id") + " to set");
            ids.add((String) document.get("_id"));
        }

        // Record time
        long endTime = System.currentTimeMillis();
        logger.debug(String.format("getExistingIds() lasted %d mills", endTime - startTime));

        return ids;
    }

    /***
     * Sleep
     *
     * Sleeps for the specified number of milliseconds, catching InterruptedException that may be thrown
     *
     * @param millis    A long representing the number of milliseconds to sleep for
     */
    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            logger.error(String.format("Encountered exception when sleeping for %d milllis: " + ex, millis));
            logger.error(ex.getMessage());
        }
    }

    /***
     * Insert Meta Data
     *
     * Records meta data about a successful BulkWrite attempt
     *
     * @param dbName
     * @param threadId
     * @param result
     */
    private void insertMetaData(String dbName, Long threadId, InsertManyResult result, Map<String, String> idsToSeqNum) {
        WriteConcern wc = new WriteConcern(0);
        MongoCollection<Document> collection = mongoClient.getDatabase(dbName)
                .getCollection(MIGRATION_COLLECTION_METADATA_NAME)
                .withWriteConcern(wc);

        Set<Document> insertedIds = new HashSet<>();
        for (BsonValue value : result.getInsertedIds().values()) {
            String seqNum = idsToSeqNum.get(value.toString());
            Document insertDoc = new Document("_id", value).append("DocumentSequenceNumber",seqNum);
            insertedIds.add(insertDoc);
        }

        Document metaDataDoc = new Document("time", new Date()).append("insertedIds", insertedIds)
                                                                .append("threadId", threadId);
        collection.insertOne(metaDataDoc);
    }

    // TODO add thread id and run information
    private void insertMetaData(String dbName, Long threadId, BulkWriteResult result, Collection<BulkWriteError> errors, Map<String, String> idsToSeqNum) {
        WriteConcern wc = new WriteConcern(0);
        MongoCollection<Document> collection = mongoClient.getDatabase(dbName)
                                                            .getCollection(MIGRATION_COLLECTION_METADATA_NAME)
                                                            .withWriteConcern(wc);

        Set<Document> insertedIds = new HashSet<>();
        for (BulkWriteInsert bulkWriteInsert : result.getInserts()) {
            String insertedId = bulkWriteInsert.getId().toString();

            String seqNum = idsToSeqNum.get(insertedId);
            Document insertDoc = new Document("_id", insertedId).append("DocumentSequenceNumber",seqNum);

            insertedIds.add(insertDoc);
        }

        Set<Document> upsertedIds = new HashSet<>();
        for (BulkWriteUpsert bulkWriteUpsert : result.getUpserts()) {
            String insertedId = bulkWriteUpsert.getId().toString();
            String seqNum = idsToSeqNum.get(insertedId);
            Document upsertDoc = new Document("_id", insertedId).append("DocumentSequenceNumber", seqNum);
            upsertedIds.add(upsertDoc);
        }

        Document metaDataDoc = new Document("time", new Date()).append("insertedIds", insertedIds)
                                                                .append("upsertedIds", upsertedIds)
                                                                .append("error", errors)
                                                                .append("threadId", threadId);
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
