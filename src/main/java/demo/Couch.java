// Copyright 2020 Kuei-chun Chen. All rights reserved.
package demo;

import org.bson.Document;
import org.ektorp.*;
import org.ektorp.http.HttpClient;
import org.ektorp.http.HttpResponse;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;
import org.json.JSONObject;
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

	public void migrate(String lastSequenceNum) throws MalformedURLException, EOFException, InterruptedException {
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

			// Get initial document ids from couch db
			ViewResult result = getInitialCouchDBDocuments(couchDB);
			mongo.insertMetaDataOperation("completeInitialCouchDBQuery", new Date());

			// Process the documents in our pool threads
			insertDataIntoMongo(executor, mongo, couchDB, result);

			// Wait for records to migrate
			waitForCompletion(executor, result.getSize(), mongo);
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
	 * Get Initial Couch DB Documents
	 *
	 * Fetches ids for all the documents from CouchDB in the database connection
	 *
	 * @param couchDB
	 * @return
	 * @throws EOFException
	 */
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
	 * Insert Data Into Mongo
	 *
	 * @param executor
	 * @param mongo
	 * @param couchDB
	 * @param initialDocumentsResult
	 * @throws InterruptedException
	 */
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

					// Record
					long startTime3 = System.currentTimeMillis();

					// Get documents from Couch DB
					ViewQuery q = new ViewQuery().allDocs().includeDocs(true).startDocId(startDocumentId).endDocId(endDocumentId)
							.inclusiveEnd(true);
					ViewResult res = couchDB.queryView(q);
					logger.debug(String.format("migrate() spent %d millis running query to retrieve docs between start id %s and end id %s from couchbase",
							System.currentTimeMillis() - startTime3,
							startDocumentId,
							endDocumentId));

					// Record
					startTime3 = System.currentTimeMillis();

					logger.debug(String.format("migrate() spent %d millis tabulating couchbase result size", System.currentTimeMillis() - startTime3));

					long id = Thread.currentThread().getId() % numThreads;
					logger.info(String.format("Starting thread with id %d", id));

					processViewResults(res, mongo, id);

					logger.info(String.format("Thread %d finished ", id));
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
				long startTime3 = System.currentTimeMillis();

				ViewQuery q = new ViewQuery().allDocs().includeDocs(true).startDocId(startDocumentId).endDocId(endDocumentId)
						.inclusiveEnd(true);
				ViewResult res = couchDB.queryView(q);
				logger.debug(String.format("migrate() spent %d millis running query to retrieve docs between start id %s and end id %s from couchbase",
						System.currentTimeMillis() - startTime3,
						startDocumentId,
						endDocumentId));

				startTime3 = System.currentTimeMillis();
				logger.debug(String.format("migrate() spent %d millis tabulating couchbase result size", System.currentTimeMillis() - startTime3));

				long id = Thread.currentThread().getId() % numThreads;
				logger.info(String.format("Starting thread with id %d", id));

				processViewResults(res, mongo, id);

				logger.info(String.format("Thread %d finished ", id));
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
	private void waitForCompletion(ThreadPoolExecutor executor, int numCouchdbDocs, Mongo mongo) throws InterruptedException {

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
	 * Process View Results
	 *
	 * @param result
	 * @param mongo
	 * @param id
	 */
	private void processViewResults(ViewResult result, Mongo mongo, long id) {
		logger.debug(String.format("Started processViewResults on thread %d and %d results", id, result.getSize()));

		long startTime = System.currentTimeMillis();

		List<Document> documents = new ArrayList<>();
		int i = 0;
		for (ViewResult.Row row : result.getRows()) {
			if (mongo.mongoContainsId(dbName, collectionName, row.getId())) {
				logger.debug(String.format("Id %s already been migrated to MongoDB. Not processing the document from Couch DB...", row.getId()));
				continue;
			}
			fetched.addAndGet(1);

			documents.add(Document.parse(row.getDoc()));
			if ((++i) % mongoBatchSize == 0) {
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

	}
}
