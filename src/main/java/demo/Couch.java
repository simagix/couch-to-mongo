// Copyright 2020 Kuei-chun Chen. All rights reserved.
package demo;

import org.bson.Document;
import org.ektorp.*;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.net.MalformedURLException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class Couch {
	private Logger logger = LoggerFactory.getLogger(Couch.class);
	private String couchdbURI;
	private int timeout;
	private String mongodbURI;
	private String dbName;
	private String collectionName;
	private int numThreads;
	private int couchBatchSize;
	private int mongoBatchSize;


	private AtomicLong fetched;
	private AtomicLong shouldInsert;
	private long initialFetchedFromMongo = 0;

	public Couch(Properties prop) {
		couchdbURI = prop.getProperty("couchdb.uri");
		mongodbURI = prop.getProperty("mongodb.uri");
		timeout = Integer.valueOf(prop.getProperty("couchdb.timeout"));
		numThreads = Integer.valueOf(prop.getProperty("num_threads"));
		couchBatchSize = Integer.valueOf(prop.getProperty("couch_batch_size"));
		mongoBatchSize = Integer.valueOf(prop.getProperty("mongo_batch_size"));
		dbName = prop.getProperty("source_database_name");
		collectionName = prop.getProperty("source_collection_name");

		fetched = new AtomicLong(0);
		shouldInsert = new AtomicLong(0);
	}

	/***
	 * Migrate
	 *
	 * Migrates data over from CouchDB to MongoDB
	 *
	 * @param lastSequenceNum				A String representing the last sequence number observed from Cloudant/CouchDB
	 * @throws MalformedURLException
	 */
	public void migrate(String lastSequenceNum) throws MalformedURLException {
		long startTime = System.currentTimeMillis();

		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThreads);
		HttpClient httpClient = new StdHttpClient.Builder().url(couchdbURI).connectionTimeout(timeout)
				.socketTimeout(timeout).build();
		CouchDbInstance dbInstance = new StdCouchDbInstance(httpClient);
		CouchDbConnector couchDB = dbInstance.createConnector(dbName, true);


		Mongo mongo = new Mongo(mongodbURI, dbName);
		try (mongo) {
			long count = mongo.countDocuments(dbName, collectionName);
			initialFetchedFromMongo = count;

			// Get partitions
			String minCouchDBKey = getMinKeyValue(couchDB);
			String maxCouchDBKey = getMaxKeyValue(couchDB);
			int numBatches = getNumBatches(minCouchDBKey, maxCouchDBKey, couchBatchSize);
			SortedMap<String, KeySpacePartition> partitions = getPartitions(minCouchDBKey, maxCouchDBKey, numBatches);

			// Get initial document ids from couch db
//			ViewResult result = getInitialCouchDBDocuments(couchDB);
//			mongo.insertMetaDataOperation("completeInitialCouchDBQuery", new Date());

			// Process the documents in our pool threads
			long numReadFromCouch = migrateInBatches(executor, mongo, couchDB, partitions);

			// Wait for records to migrate
			waitForCompletion(executor, numReadFromCouch, mongo);
			mongo.insertMetaDataOperation("end", new Date());

			long inMongo = mongo.countDocuments(dbName, collectionName);
			logger.debug(String.format("migrate() spent %d millis total migrating %d documents", System.currentTimeMillis() - startTime, inMongo));

			// Start the Change feed processing
			ChangeFeedClient client = new ChangeFeedClient(lastSequenceNum, mongo, dbName, collectionName, couchDB);
			client.applyChanges();

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	/***
	 * Get Min Key Value
	 *
	 * Gets the minimum value of the key (_id) among documents in the source CouchDB database
	 *
	 * @param couchDB		A CouchDbConnector instance
	 * @return				A String representing the minimum key in the source CouchDB database
	 */
	private String getMinKeyValue(CouchDbConnector couchDB) {
		logger.info("Attempting to find min _id field in CouchDB...");

		// Record
		long startTime2 = System.currentTimeMillis();

		// Get all documents from Couch DB; sorted in ascending order of _id by default
		ViewQuery query = new ViewQuery().allDocs().includeDocs(false).limit(1);
		ViewResult result = couchDB.queryView(query);
		logger.debug(String.format("migrate() spent %d mills while running initial query", System.currentTimeMillis() - startTime2));

		logger.info(String.format("found %d documents in CouchDB", result.getSize()));

		if (result.isEmpty()) {
			return "";
		}

		for (ViewResult.Row row : result.getRows()) {
			String minKeyValue = row.getId();
			return minKeyValue;
		}
		return null;
	}

	/***
	 * Get Max Key Value
	 *
	 * @param couchDB		A CouchDbConnector instance
	 * @return				A String representing the minimum key in the source CouchDB database
	 */
	private String getMaxKeyValue(CouchDbConnector couchDB) {
		logger.info("Attempting to find max _id field in CouchDB...");

		// Record
		long startTime2 = System.currentTimeMillis();

		// Get all documents from Couch DB
		ViewQuery query = new ViewQuery().allDocs().includeDocs(false).descending(true).limit(1);
		ViewResult result = couchDB.queryView(query);
		logger.debug(String.format("migrate() spent %d mills while running initial query", System.currentTimeMillis() - startTime2));

		logger.info(String.format("found %d documents in CouchDB", result.getSize()));

		if (result.isEmpty()) {
			return "";
		}

		for (ViewResult.Row row : result.getRows()) {
			String maxKeyValue = row.getId();
			return maxKeyValue;
		}
		return null;
	}

	/***
	 * Get Number of Batches
	 *
	 * Calculates the number of batches given the key range and batch size
	 *
	 * @param minKey			A String representation of the min document id
	 * @param maxkey			A String representation of the maximum document id
	 * @param maxBatchSize		An integer representing the max number of documents/ids per batch
	 * @return					An integer representing the number of resulting batches needed to handle specified key-range
	 */
	private int getNumBatches(String minKey, String maxkey, int maxBatchSize) {
		logger.info(String.format("Calculating number of batchs for key range [%s,%s) and max batch size of %d", minKey, maxkey, maxBatchSize));
		Integer minKeyInt = Integer.parseInt(minKey);
		Integer maxKeyInt = Integer.parseInt(maxkey);
		return (int) Math.ceil((maxKeyInt - minKeyInt)/maxBatchSize);
	}

	/***
	 * Get Partitions
	 *
	 * Computes a map of minKey --> KeySpacePartition objects representing all of the key space ranges that will be parallelized.
	 * The KeySpacePartition object is essentially a tuple representing [minKey,maxKey) ranges.
	 *
	 * @param minKey			A String representing the min key of the document keyspace
	 * @param maxkey			A String representing the max key of the document keyspace
	 * @param numPartitions		An integer representing the number of partitions desired
	 * @return 					A SortedMap<String, KeySpacePartition> representing all the ordered key ranges
	 */
	private SortedMap<String, KeySpacePartition> getPartitions(String minKey, String maxkey, int numPartitions) {
		logger.info(String.format("Calculating %d partitions for range [%s,%s)",numPartitions,  minKey, maxkey));

		SortedMap<String, KeySpacePartition> partitions = new TreeMap<>();

		Integer minKeyInt = Integer.parseInt(minKey);
		Integer maxKeyInt = Integer.parseInt(maxkey);

		int maxNumKeysPerPartition = (int) Math.ceil((maxKeyInt - minKeyInt)/numPartitions);
		logger.debug("Calculated " + maxNumKeysPerPartition + " max number of keys per partitions");

		// Example:
		// Keyspace : 0 - 23, numPartitions = 5 --> maxNumKeysPerPartition = CEIL(23/5) = CEIL(4.6) = 5
		// Partition 0: [0, 5)
		// Partition 1: [5, 10)
		// Partition 2: [10, 15)
		// Partition 3: [15, 20)
		// Partition 4: [20, 25)
		Integer lastKeySeen = minKeyInt;
		for (int i = 0; i < numPartitions; i++) {
			Integer currentKey = lastKeySeen + maxNumKeysPerPartition;

			logger.debug(String.format("Creating partition [%s,%s)", lastKeySeen.toString(), currentKey.toString()));
			KeySpacePartition partition = new KeySpacePartition(lastKeySeen.toString(), currentKey.toString());
			partitions.put(currentKey.toString(), partition);

			lastKeySeen = currentKey;
		}
		return partitions;
	}

	/***
	 * Get Initial Couch DB Documents
	 *
	 * Fetches ids for all the documents from CouchDB in the database connection
	 *
	 * @param couchDB			A CouchDbConnector instance
	 * @return					A CouchDB ektorp client ViewResult instance containing all ids for all documents in the
	 * 							CouchDB database
	 * @throws EOFException
	 */
	@Deprecated
	private ViewResult getInitialCouchDBDocuments(CouchDbConnector couchDB) throws EOFException {
		logger.info("Sending initial query to CouchDB to get the set of document ids");

		// Record
		long startTime2 = System.currentTimeMillis();

		// Get all documents from Couch DB
		ViewQuery query = new ViewQuery().allDocs().includeDocs(false);
		ViewResult result = couchDB.queryView(query);
		logger.debug(String.format("migrate() spent %d mills while running initial query", System.currentTimeMillis() - startTime2));

		logger.info(String.format("found %d documents in CouchDB", result.getSize()));

		if (result.isEmpty()) {
			throw new EOFException("no document found");
		}
		return result;
	}

	/***
	 * Insert Data Into Mongo With Partitions
	 *
	 * Takes a set of KeySpacePartition objects and a ThreadPoolExecutor and starts a thread on each partition to migrate
	 * data from CouchDB to MongoDB in that range
	 *
	 * @param executor			A ThreadPoolExecutor object containing all the threads to run the migration on
	 * @param mongo				A Mongo object representing a MongoDB client
	 * @param couchDB			An ektorp CouchDBConnector object representing a CouchDB client
	 * @param partitionMap		A SortedMap<String, KeySpacePartition> repersenting a map of min key to KeySpace Partition objects
	 * @return					A long representing the number of documents the job will migrate
	 * @throws InterruptedException
	 */
	private long migrateInBatches(ThreadPoolExecutor executor, Mongo mongo, CouchDbConnector couchDB, SortedMap<String, KeySpacePartition> partitionMap) {
		logger.info("Migrating data in batches based on partitions...");

		AtomicLong numRead = new AtomicLong(0);

		for (String minKey : partitionMap.keySet()) {

			KeySpacePartition partition = partitionMap.get(minKey);

			logger.info(String.format("Creating migration batch for key range [%s,%s)", partition.getMinKey(), partition.getMaxKey()));
			executor.submit(() -> {
				int numReadFromCouch = fetchFromCouchDBAndMigrate(partition.getMinKey(), partition.getMaxKey(), couchDB, mongo);
				numRead.addAndGet(numReadFromCouch);
			});

			while (executor.getQueue().size() > numThreads) {
				logger.info(String.format("thread has %d jobs in queue, throttling", executor.getQueue().size()));
				try {
					Thread.sleep(5000);    // throttle and yield
				} catch (InterruptedException ex) {
					logger.error("Encountered an exception when attempting to sleep: " + ex);
				}
			}
		}

		return numRead.get();
	}

	/***
	 * Insert Data Into Mongo
	 *
	 * @param executor
	 * @param mongo
	 * @param couchDB
	 * @param initialDocumentsResult
	 * @throws InterruptedException
	 */
	@Deprecated
	private void insertDataIntoMongo(ThreadPoolExecutor executor, Mongo mongo, CouchDbConnector couchDB, ViewResult initialDocumentsResult) throws InterruptedException {

		int counter = 0;
		int docsFetched = 0;
		String startDocId = null;
		String endDocId = null;

		// Divide into batches
		for (ViewResult.Row row : initialDocumentsResult.getRows()) {

			if (mongo.mongoContainsId(dbName, collectionName, row.getId())) {
				logger.debug(String.format("Id %s already been migrated to MongoDB. Skipping...", row.getId()));
				continue;
			}

			counter++;
			endDocId = row.getId();
			if (counter == 1) {
				startDocId = row.getId();
				counter++;
			} else if (counter == couchBatchSize) {

				final String startDocumentId = startDocId;
				final String endDocumentId = endDocId;

				executor.submit(() -> {
					 fetchFromCouchDBAndMigrate(startDocumentId, endDocumentId, couchDB, mongo);
				});

				// Need to log this to understand how far behind we are and if the migration client cannot keep up with the load
				logger.info(String.format("fetching, total of %d fetched, %d inserted", fetched.get(), shouldInsert.get()));

				counter = 0;
				while (executor.getQueue().size() > numThreads) {
					logger.info(String.format("thread has %d jobs in queue, throttling", executor.getQueue().size()));
					Thread.sleep(5000);    // throttle and yield
				}
			}
		}

		// Insert remaining docs
		if (counter > 0) {

			final String startDocumentId = startDocId;
			final String endDocumentId = endDocId;

			executor.submit(() -> {
				fetchFromCouchDBAndMigrate(startDocumentId, endDocumentId, couchDB, mongo);
			});
		}

		// Need to log this to understand how far behind we are and if the migration client cannot keep up with the load
		logger.info(String.format("end of fetching, total of %d fetched, %d inserted", fetched.get(), shouldInsert.get()));
		logger.info(String.format("last batch start key: %s, end key: %s", startDocId, endDocId));
	}

	/***
	 * Wait for Completion
	 *
	 * Waits for the appropriate number of documents to be in MongoDB before forcibly closing the executor thread pool
	 *
	 * @param executor
	 * @param mongo
	 * @throws InterruptedException
	 */
	private void waitForCompletion(ThreadPoolExecutor executor, long numCouchdbDocs, Mongo mongo) throws InterruptedException {

		long inMongo = mongo.countDocuments(dbName, collectionName);

		while (numCouchdbDocs != inMongo) {

			logger.info(String.format("total of %d fetched, %d in mongo", numCouchdbDocs, inMongo));
			Thread.sleep(5000);
			inMongo = mongo.countDocuments(dbName, collectionName);
		}
		executor.shutdown();
		executor.awaitTermination(5, TimeUnit.SECONDS);
	}

	/***
	 * fetchFromCouchDBAndMigrate
	 *
	 * Read data for the start/end range and migrate to MongoDB
	 *
	 * @param startDocumentId			A String representing the _id of the first document in this range to migrate
	 * @param endDocumentId				A String representing the _id of the last document in this range to migrate
	 * @param couchDB					A CouchDBConnector instance
	 * @param mongo						A Mongo instance
	 */
	private int fetchFromCouchDBAndMigrate(String startDocumentId, String endDocumentId, CouchDbConnector couchDB, Mongo mongo) {
		logger.info("Fetching data from CouchDB and migrating");

		// Record
		long startTime3 = System.currentTimeMillis();

		// Get documents from Couch DB
		ViewQuery q = new ViewQuery().allDocs().includeDocs(true).startDocId(startDocumentId).endDocId(endDocumentId)
				.inclusiveEnd(false);
		ViewResult res = couchDB.queryView(q);
		int resultSize = res.getSize();
		logger.debug(String.format("migrate() spent %d millis running query to retrieve %d docs between start id %s and end id %s from couchbase",
				System.currentTimeMillis() - startTime3,
				resultSize,
				startDocumentId,
				endDocumentId));

		// Record
		startTime3 = System.currentTimeMillis();

		logger.debug(String.format("migrate() spent %d millis tabulating couchbase result size", System.currentTimeMillis() - startTime3));

		long id = Thread.currentThread().getId() % numThreads;
		logger.info(String.format("Starting thread with id %d", id));

		processViewResults(res, mongo, id);
		logger.info(String.format("Thread %d finished ", id));

		return resultSize;
	}

	/***
	 * Process View Results
	 *
	 * Process view results from CouchDB by inserting them into MongoDB
	 *
	 * @param result            A ektorp CouchDB client ViewResult object, containing results from the query to process
     *                          and insert into MongoDB
	 * @param mongo             A Mongo object representing a MongoDB client
	 * @param id                The thread id, or other id used for logging and auditing
	 */
	private int processViewResults(ViewResult result, Mongo mongo, long id) {
		logger.debug(String.format("Started processViewResults on thread %d and %d results", id, result.getSize()));

		long startTime = System.currentTimeMillis();

		List<Document> documents = new ArrayList<>();
		int numShouldMigrate = 0;
		for (ViewResult.Row row : result.getRows()) {
			if (mongo.mongoContainsId(dbName, collectionName, row.getId())) {
				logger.debug(String.format("Id %s already been migrated to MongoDB. Not processing the document from Couch DB...", row.getId()));
				continue;
			}
			fetched.addAndGet(1);

			documents.add(Document.parse(row.getDoc()));
			if ((++numShouldMigrate) % mongoBatchSize == 0) {
				int saved = mongo.saveToMongo(this.dbName, this.collectionName, documents, id);
				String message = String.format("[%d] %d sent, %d inserted to mongo", id, documents.size(), saved);
				logger.info(message);

				shouldInsert.addAndGet(saved);
				documents = new ArrayList<>();
			}
		}
		if (!documents.isEmpty()) {
			int saved = mongo.saveToMongo(this.dbName, this.collectionName, documents, id);
			String message = String.format("[%d] %d sent, %d inserted to mongo", id, documents.size(), saved);
			logger.info(message);

			shouldInsert.addAndGet(saved);
		}

		logger.debug(String.format("Returning from processViewResults on thread %d", id));

		// Record time
		long endTime = System.currentTimeMillis();
		logger.debug(String.format("processViewResults() on id %d lasted %d mills", id, endTime - startTime));
		return numShouldMigrate;
	}

	/***
	 * KeySpace Partition
	 *
	 * A Private class that represents a key range (lower-bound inclusive) for a set of documents in any structure, but
	 * particularly for a CouchDB database.
	 *
	 */
	private class KeySpacePartition {
		private String minKey;
		private String maxKey;

		public KeySpacePartition(String minKey, String maxKey) {
			this.minKey = minKey;
			this.maxKey = maxKey;
		}

		public String getMinKey() {
			return minKey;
		}

		public String getMaxKey() {
			return maxKey;
		}
	}
}
