// Copyright 2020 Kuei-chun Chen. All rights reserved.
package demo;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.bson.Document;
import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.ViewQuery;
import org.ektorp.ViewResult;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Couch {
	private Logger logger = LoggerFactory.getLogger(Couch.class);
    private final int MAX_NUM_READ_ATTEMPTS = 10;
	private String couchdbURI;
	private int timeout;
	private String mongodbURI;
	private String dbName;
	private String collectionName;
	private int numThreads;
	private int couchBatchSize;
	private int mongoBatchSize;
	
	private SortedMap<String, KeySpacePartition> partitionsMap;
	private AtomicLong numRead = new AtomicLong(0);
	private AtomicLong numProcessed = new AtomicLong(0);
	private AtomicLong fetched;
	private AtomicLong shouldInsert;

	private Map<String, Boolean> idProcessed;

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

		idProcessed = new ConcurrentHashMap<>();
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
			// Get partitions
			partitionsMap = getPartitions(couchDB, couchBatchSize);

			// Process the documents in our pool threads
			migrateInBatches(executor, mongo, couchDB, partitionsMap);

			// Wait for records to migrate
			waitForCompletion(executor, mongo);
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

	private SortedMap<String, KeySpacePartition> getPartitions(CouchDbConnector couchDB, int couchBatchSize) {
		logger.info("Calculating partitions with batch size of " + couchBatchSize);
		SortedMap<String, KeySpacePartition> partitions = new TreeMap<>();
		long t = System.currentTimeMillis();
		ViewQuery query = new ViewQuery().allDocs().includeDocs(false).limit(1);
		ViewResult result = couchDB.queryView(query);
		logger.debug(String.format("migrate() spent %d mills while running initial query", System.currentTimeMillis() - t));
		if (result.isEmpty()) {
			return partitions;
		}
		String minKeyValue = result.getRows().get(0).getId();
		String maxKeyValue = "";
		logger.info("have minKeyValue: "+ minKeyValue);
		while(true) {
			ViewQuery q = new ViewQuery().allDocs().includeDocs(false).startDocId(minKeyValue).limit(couchBatchSize);
			ViewResult res = couchDB.queryView(q);
			if(res.isEmpty()) {
				break;
			}
			int size = res.getSize();
			maxKeyValue = res.getRows().get(size-1).getId();
			logger.debug(String.format("partition (%s, %s), size: %d", minKeyValue, maxKeyValue, size));
			KeySpacePartition partition = new KeySpacePartition(minKeyValue, maxKeyValue);
			partitions.put(minKeyValue+"-"+maxKeyValue, partition);
			if(partitions.size()%100 == 0) {
				logger.info(String.format("partitions created %d", partitions.size()));
			}
			if(minKeyValue.equals(maxKeyValue)) {
				break;
			}
			minKeyValue = maxKeyValue;
		}
		logger.info(String.format("total partitions created %d", partitions.size()));
		return partitions;
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
	 * @throws InterruptedException
	 */
	private void migrateInBatches(ThreadPoolExecutor executor, Mongo mongo, CouchDbConnector couchDB, SortedMap<String, KeySpacePartition> partitionMap) {
		logger.info("Migrating data in batches based on partitions...");
		for (Map.Entry<String, KeySpacePartition> entry : partitionMap.entrySet()) {
			KeySpacePartition partition = entry.getValue();
			logger.info(String.format("Creating migration batch for key range (%s,%s)", partition.getMinKey(), partition.getMaxKey()));
			executor.submit(() -> {
				try {
					long id = Thread.currentThread().getId() % numThreads;
					logger.debug(String.format("Starting thread with id %d", id));
					int count = 0;
					while(count++ < MAX_NUM_READ_ATTEMPTS) {
						try {
							fetchFromCouchDBAndMigrate(partition.getMinKey(), partition.getMaxKey(), couchDB, mongo);
							count = MAX_NUM_READ_ATTEMPTS;
						} catch(Exception ex) {
							logger.error(String.format("[%d] fetchFromCouchDBAndMigrate: ", id) + ex);
							sleep(30000);
							continue;
						}
					}
					logger.debug(String.format("Thread %d finished ", id));
				} finally {
					numProcessed.addAndGet(1);
				}
			});

			while (executor.getQueue().size() > numThreads) {
				logger.debug(String.format("thread has %d jobs in queue, throttling", executor.getQueue().size()));
				try {
					Thread.sleep(5000);    // throttle and yield
				} catch (InterruptedException ex) {
					logger.error("Encountered an exception when attempting to sleep: " + ex);
				}
			}
		}
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
	private void waitForCompletion(ThreadPoolExecutor executor, Mongo mongo) throws InterruptedException {

		long inMongo = 0;

		do {
			inMongo = mongo.countDocuments(dbName, collectionName);
			logger.info(String.format("%d threads active, %d tasks queued, %d of %d processed", 
				executor.getActiveCount(), executor.getQueue().size(), numProcessed.get(), partitionsMap.size()));
			logger.info(String.format("total of %d fetched, %d in mongo", numRead.get(), inMongo));
			Thread.sleep(5000);
		} while (numProcessed.get() < partitionsMap.size());

		logger.info(String.format("%d of %d processed", numProcessed.get(), partitionsMap.size()));
		inMongo = mongo.countDocuments(dbName, collectionName);
		logger.info(String.format("total of %d fetched, %d in mongo", numRead.get(), inMongo));
		logger.info("shutdown thread executor");
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
	private void fetchFromCouchDBAndMigrate(String startDocumentId, String endDocumentId, CouchDbConnector couchDB, Mongo mongo) {
		long id = Thread.currentThread().getId() % numThreads;
		logger.debug("Fetching data from CouchDB and migrating");

		// Record
		long startTime3 = System.currentTimeMillis();

		// Get documents from Couch DB
		boolean incluseive = (startDocumentId.equals(endDocumentId));
		ViewQuery q = new ViewQuery().allDocs().includeDocs(true).startDocId(startDocumentId).endDocId(endDocumentId).inclusiveEnd(incluseive);
		ViewResult res = couchDB.queryView(q);

		// Do not double-count rows
		int resultSize = 0;
		for (ViewResult.Row row : res.getRows()) {
			if (!hasIdBeenReadFromCouch(row)) {
				resultSize++;
			}
		}
		numRead.addAndGet(resultSize);

		logger.debug(String.format("migrate() spent %d millis running query to retrieve %d docs between start id %s and end id %s from couchbase",
				System.currentTimeMillis() - startTime3,
				resultSize,
				startDocumentId,
				endDocumentId));

		// Record
		startTime3 = System.currentTimeMillis();

		logger.debug(String.format("migrate() spent %d millis tabulating couchbase result size", System.currentTimeMillis() - startTime3));

		if (resultSize == 0) {
			logger.debug(String.format("Already migrated all documents in partition range [%s,%s).", startDocumentId, endDocumentId));
			return;
		}

		processViewResults(res, mongo, id);
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
	private void processViewResults(ViewResult result, Mongo mongo, long id) {
		logger.info(String.format("Started processViewResults on thread %d and %d results", id, result.getSize()));

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
				logger.debug(message);

				shouldInsert.addAndGet(saved);
				documents = new ArrayList<>();
			}
		}
		if (!documents.isEmpty()) {
			int saved = mongo.saveToMongo(this.dbName, this.collectionName, documents, id);
			String message = String.format("[%d] %d sent, %d inserted to mongo", id, documents.size(), saved);
			logger.debug(message);

			shouldInsert.addAndGet(saved);
		}

		logger.debug(String.format("Returning from processViewResults on thread %d", id));

		// Record time
		long endTime = System.currentTimeMillis();
		logger.debug(String.format("processViewResults() on id %d lasted %d mills", id, endTime - startTime));
	}

	private boolean hasIdBeenReadFromCouch(ViewResult.Row row) {
		try {

			String docStr = row.getDoc();
			Document doc = Document.parse(docStr);
			String docId = doc.getString("_id");

			boolean hasBeenProcessed = false;
			if (idProcessed.containsKey(docId)) {
				hasBeenProcessed = idProcessed.get(docId);
			}

			if (!hasBeenProcessed) {
				idProcessed.put(docId, true);
			}
			return hasBeenProcessed;

		} catch (Exception ex) {
			logger.error("Encountered exception: " + ex);
		}
		return false;
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

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            logger.error(String.format("Encountered exception when sleeping for %d milllis: " + ex, millis));
            logger.error(ex.getMessage());
        }
    }
}
