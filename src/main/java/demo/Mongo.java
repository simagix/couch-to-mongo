// Copyright 2020 Kuei-chun Chen. All rights reserved.
package demo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteInsert;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.UpdateResult;

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

			// Create empty set of existing Ids, as we will not need to find existing
			// documents in MongoDB, because this
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
	 * Checks if a given document id already exists in Mongo (and thus was already
	 * migrated).
	 *
	 * @param dbName         A String representing the database to check in
	 * @param collectionName A String representing the collection to check in
	 * @param id             A String representing the id whose presence to check
	 * @return true if the id is present in the designated MongoDB namespace, false
	 *         otherwise
	 */
	public boolean mongoContainsId(String dbName, String collectionName, String id) {
		// Lazily initialize
		if (existingIds == null) {
			synchronized (this) {
				logger.debug("Lazily initializing existing ids...");
				existingIds = getExistingIds(dbName, collectionName);
			}
		}
		return existingIds.contains(id);
	}

	/***
	 * Get UUID Session Id
	 *
	 * Gets the UUID for the session for this migration run
	 *
	 * @return A UUID
	 */
	public UUID getSessionId() {
		return sessionId;
	}

	public int updateDocsInMongo(String dbName, String collectionName, Set<Document> documents, Long threadId) {
		long startTime = System.currentTimeMillis();

		logger.debug("Updating documents in mongo");
		if (documents.isEmpty()) {
			logger.debug("Result set provided is empty. Not inserting.");
			return 0;
		}

		MongoCollection<Document> collection = mongoClient.getDatabase(dbName).getCollection(collectionName);
		BulkWriteOptions options = new BulkWriteOptions().ordered(false);

		Map<String, String> docIdToSeqNum = getDocumentSequenceNums(documents);

		List<WriteModel<Document>> updates = createWriteModelsForMongo(documents);

		int resultSize = 0;
		int numUpdateAttempts = 0;

		while (numUpdateAttempts < MAX_NUM_INSERT_ATTEMPTS) {
			numUpdateAttempts++;
			logger.trace(String.format("Making update attempt %d/%d", numUpdateAttempts, MAX_NUM_INSERT_ATTEMPTS));
			try {
				String message = String.format("documents size %d", updates.size());
				logger.debug(message);

				BulkWriteResult result = collection.bulkWrite(updates, options);
				resultSize = result.getModifiedCount() + result.getInsertedCount() + result.getDeletedCount() +
						result.getUpserts().size();

				logger.info(String.format(
						"Successfully matched %d documents, updated %d documents, "
								+ "upserted %d documents, and deleted %d documents",
						result.getMatchedCount(), result.getModifiedCount(), result.getUpserts().size(),
						result.getDeletedCount()));

				if (resultSize > 0) {
					insertMetaData(dbName, threadId, result, docIdToSeqNum);
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
		logger.debug(String.format("updateDocsInMongo() lasted %d mills", endTime - startTime));

		return resultSize;
	}

	private List<WriteModel<Document>> createWriteModelsForMongo(Set<Document> documents) {

		List<WriteModel<Document>> updates = new ArrayList<>();
		ReplaceOptions replaceOptions = new ReplaceOptions().upsert(true);

		for (Document document : documents) {

			String id = document.get("_id").toString();
			Document queryDoc = new Document("_id", id);

			boolean isDeletedFromCloudant = false;
			if (document.containsKey("_deleted")) {
				isDeletedFromCloudant = document.getBoolean("_deleted");
			}

			if (isDeletedFromCloudant) {
				updates.add(new DeleteOneModel(queryDoc));
			} else {
				updates.add(new ReplaceOneModel(queryDoc, document, replaceOptions));
			}
		}
		return updates;
	}

	public int saveToMongo(String dbName, String collectionName, List<Document> documents, Long threadId) {
		long startTime = System.currentTimeMillis();

		logger.debug("Saving data to mongo");

		if (documents.isEmpty()) {
			logger.trace("Result set provided is empty. Not inserting.");
			return 0;
		}

		Map<String, String> docIdToSeqNum = getDocumentSequenceNums(documents);

		MongoCollection<Document> collection = mongoClient.getDatabase(dbName).getCollection(collectionName);
		InsertManyOptions options = new InsertManyOptions();
		options.ordered(false);

		int resultSize = 0;
		int numInsertAttempts = 0;
		boolean inserted = true;

		while (numInsertAttempts < MAX_NUM_INSERT_ATTEMPTS) {
			inserted = false;
			numInsertAttempts++;
			logger.trace(String.format("Making write attempt %d/%d", numInsertAttempts, MAX_NUM_INSERT_ATTEMPTS));
			try {
				String message = String.format("documents size %d", documents.size());
				logger.debug(message);

				InsertManyResult res = collection.insertMany(documents, options);
				resultSize = res.getInsertedIds().size();
				logger.debug(String.format("Successfully inserted %d documents", resultSize));

				if (resultSize > 0) {
					inserted = true;
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

		if (!inserted) {
			if (documents.size() > 1) {
				for (Document doc : documents) {
					List<Document> single = new ArrayList<Document>(1);
					single.add(doc);
					saveToMongo(dbName, collectionName, single, threadId);
				}
			} else if (documents.size() == 1) {
				logger.error("insert failed _id " + documents.get(0).getString("_id"));
				//logger.error(documents.get(0).toJson());
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
				numDocs = collection.estimatedDocumentCount();
				break;
			} catch (MongoException ex) {
				logger.error("Encountered MongoBulkWriteException: " + ex);
				logger.error(ex.getMessage());
				sleep(2000);
				continue;
			} catch (Exception ex) {

				logger.error("Encountered exception: " + ex);
				logger.error(ex.getMessage());
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
	 * @param logMessage A String representing the message to log
	 * @param date       A Date object representing the timestamp of the log message
	 */
	public void insertMetaDataOperation(String logMessage, Date date) {

		// WC = 0 is effectively an asynchronous write. Metadata ops should be async to
		// maintain performance
		WriteConcern wc = new WriteConcern(0);
		MongoCollection<Document> collection = mongoClient.getDatabase(dbName)
				.getCollection(MIGRATION_COLLECTION_METADATA_NAME).withWriteConcern(wc);

		Document startTimeDoc = new Document("operation", logMessage).append("time", date).append("session",
				sessionId.toString());
		collection.insertOne(startTimeDoc);
	}

	/****
	 * Insert Last Sequence Number
	 *
	 * Inserts the last sequence number (from Couch DB) for a particular migration
	 * run, if it doesn't already exist for the run (as identified by the migration
	 * run's session id).
	 *
	 * @param lastSequenceNumber
	 * @param date
	 */
	public void insertLastSequenceNumber(String lastSequenceNumber, Date date) {
		if (lastSequenceNumber == null) {
			logger.debug("Received null lastSequenceNumber; cannot proceed to log this.");
			return;
		}

		WriteConcern wc = new WriteConcern(1);
		MongoCollection<Document> collection = mongoClient.getDatabase(dbName)
				.getCollection(MIGRATION_COLLECTION_METADATA_NAME).withWriteConcern(wc);

		Document documentCountQueryDoc = new Document("session", sessionId.toString()).append("operation", "logLastSequenceNumber");
		boolean isUpsertEnabled =false;

		long numLogLastSeqNumDocs = collection.countDocuments(documentCountQueryDoc);
		if (numLogLastSeqNumDocs == 0) {
			isUpsertEnabled = true;
		}

		Document queryDoc = new Document("session", sessionId.toString()).append("operation", "logLastSequenceNumber")
				.append("lastSequenceNumber", new Document("$ne", lastSequenceNumber));
		Document lastSeqNumDoc = new Document("operation", "logLastSequenceNumber").append("time", date)
				.append("session", sessionId.toString()).append("lastSequenceNumber", lastSequenceNumber);
		Document updateDoc = new Document("$set", lastSeqNumDoc);

		UpdateOptions updateOptions = new UpdateOptions().upsert(isUpsertEnabled);

		UpdateResult updateResult = collection.updateOne(queryDoc, updateDoc, updateOptions);
		if (updateResult.getModifiedCount() > 0) {
			logger.debug("Persisting last sequence number to " + lastSequenceNumber);
		}



	}

	public String getLastSequenceNumber() {
		WriteConcern wc = new WriteConcern(0);
		MongoCollection<Document> collection = mongoClient.getDatabase(dbName)
				.getCollection(MIGRATION_COLLECTION_METADATA_NAME).withWriteConcern(wc);

		Document queryDoc = new Document("operation", "logLastSequenceNumber");
		Document sortDoc = new Document("time", -1);

		FindIterable<Document> cursor = collection.find(queryDoc).sort(sortDoc).limit(1);
		for (Document document : cursor) {
			String sequenceNumber = document.getString("lastSequenceNumber");
			return sequenceNumber;
		}
		return ChangeFeedClient.NO_SEQUENCE_NUMBER_FOUND;
	}

	/***
	 * Get Document Sequence Numbers
	 *
	 * Fetches a Map<String, String> representing the DocumentSequenceNumber for
	 * each document inserted
	 *
	 * @param documents A List<Document> containing the documents prior to inserting
	 *                  them into the MongoDB collection
	 * @return A Map<String, String> representing the DocumentSequenceNumber for
	 *         each document inserted
	 */
	private Map<String, String> getDocumentSequenceNums(Collection<Document> documents) {
		logger.debug("Getting document sequence numbers");
		Map<String, String> docIdToSeqNum = new HashMap<>();

		try {
			for (Document document : documents) {
				logger.trace("Looking for Header field in document " + document);

				String id = (String) document.get("_id");

				String documentSeqNum;

				try {
					Document nestedDoc = (Document) document.get("Header");
					Double docSeqNumDouble = nestedDoc.getDouble("DocumentSequenceNumber");
					Long docSeqNum = docSeqNumDouble.longValue();
					documentSeqNum = docSeqNum.toString();

				} catch (Exception ex) {
					logger.debug(String.format("Nested document HEADER was null, id: %s", id));
					documentSeqNum = "";
				}

				docIdToSeqNum.put(id, documentSeqNum);
				logger.trace(String.format("Inserting document sequence number %s for id %s", documentSeqNum, id));
			}
		} catch (Exception ex) {
			logger.error("Encountered error when attempting to fetch ids for documents: " + ex.getMessage(), ex);
		}
		return docIdToSeqNum;
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
				.getCollection(MIGRATION_COLLECTION_METADATA_NAME).withWriteConcern(wc);

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

		Document groupStage = new Document("$group",
				new Document("_id", "$session").append("operations", new Document("$push", "$operation"))
						.append("startTime", new Document("$first", "$time"))
						.append("endTime", new Document("$last", "$time")));
		getSessionsPipelineStages.add(groupStage);

		Document sortByStartTimeInDescendingOrderStage = new Document("$sort", new Document("startTime", -1));
		getSessionsPipelineStages.add(sortByStartTimeInDescendingOrderStage);

		Document limitToOneSessionStage = new Document("$limit", 1);
		getSessionsPipelineStages.add(limitToOneSessionStage);

		AggregateIterable<Document> sessionsData = collection.aggregate(getSessionsPipelineStages);
		for (Document sessionDataDocument : sessionsData) {

			List<String> operations = (List<String>) sessionDataDocument.get("operations");

			// If there is no end operation, then we know that this session was interrupted
			if (!operations.contains("end")) {
				return UUID.fromString((String) sessionDataDocument.get("_id"));
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
	public Set<String> getExistingIds(String dbName, String collectionName) {
		long startTime = System.currentTimeMillis();

		logger.debug(String.format("Getting existing ids in MongoDB namespace %s.%s", dbName, collectionName));
		Set<String> ids = new HashSet<>();

		MongoCollection<Document> collection = mongoClient.getDatabase(dbName).getCollection(collectionName);
		Document queryAllDoc = new Document();
		Document projectionOnlyIdDocument = new Document("_id", 1);
		Document sortOnIdDoc = new Document("_id", 1);

		FindIterable<Document> cursor = collection.find(queryAllDoc).projection(projectionOnlyIdDocument)
				.sort(sortOnIdDoc);
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
	 * Sleeps for the specified number of milliseconds, catching
	 * InterruptedException that may be thrown
	 *
	 * @param millis A long representing the number of milliseconds to sleep for
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
	private void insertMetaData(String dbName, Long threadId, InsertManyResult result,
			Map<String, String> idsToSeqNum) {
		WriteConcern wc = new WriteConcern(0);
		MongoCollection<Document> collection = mongoClient.getDatabase(dbName)
				.getCollection(MIGRATION_COLLECTION_METADATA_NAME).withWriteConcern(wc);

		Set<Document> insertedIds = new HashSet<>();
		for (BsonValue value : result.getInsertedIds().values()) {
			String valueString = ((BsonString) value).getValue();

			String seqNum = idsToSeqNum.get(valueString);
			Document insertDoc = new Document("_id", valueString).append("DocumentSequenceNumber", seqNum);
			insertedIds.add(insertDoc);
		}

		Document metaDataDoc = new Document("time", new Date()).append("insertedIds", insertedIds).append("threadId",
				threadId);
		collection.insertOne(metaDataDoc);
	}

	private void insertMetaData(String dbName, Long threadId, BulkWriteResult result, Map<String, String> idsToSeqNum) {
		WriteConcern wc = new WriteConcern(0);
		MongoCollection<Document> collection = mongoClient.getDatabase(dbName)
				.getCollection(MIGRATION_COLLECTION_METADATA_NAME).withWriteConcern(wc);

		Set<Document> upsertedIds = new HashSet<>();
		for (BulkWriteUpsert upsert : result.getUpserts()) {
			String valueString = ((BsonString) upsert.getId()).getValue();

			String seqNum = idsToSeqNum.get(valueString);
			Document insertDoc = new Document("_id", valueString).append("DocumentSequenceNumber", seqNum);
			upsertedIds.add(insertDoc);
		}

		Document metaDataDoc = new Document("time", new Date()).append("upsertedIds", upsertedIds).append("threadId",
				threadId);
		collection.insertOne(metaDataDoc);
	}

	// TODO add thread id and run information
	private void insertMetaData(String dbName, Long threadId, BulkWriteResult result, Collection<BulkWriteError> errors,
			Map<String, String> idsToSeqNum) {
		WriteConcern wc = new WriteConcern(0);
		MongoCollection<Document> collection = mongoClient.getDatabase(dbName)
				.getCollection(MIGRATION_COLLECTION_METADATA_NAME).withWriteConcern(wc);

		Set<Document> insertedIds = new HashSet<>();
		for (BulkWriteInsert bulkWriteInsert : result.getInserts()) {
			String insertedId = bulkWriteInsert.getId().toString();

			String seqNum = idsToSeqNum.get(insertedId);
			Document insertDoc = new Document("_id", insertedId).append("DocumentSequenceNumber", seqNum);

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
				.append("upsertedIds", upsertedIds).append("error", errors).append("threadId", threadId);
		collection.insertOne(metaDataDoc);
	}

	// TODO : may not need this
	private Set<Document> getUninsertedDocuments(Set<Document> documents, InsertManyResult result) {

		Collection<BsonValue> insertedIds = result.getInsertedIds().values();
		Set<Document> newDocuments = new HashSet<>();
		for (Document document : documents) {
			if (insertedIds.contains((BsonValue) document.get("_id"))) {
				continue;
			}
		}
		return newDocuments;
	}
}