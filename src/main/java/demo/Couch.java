// Copyright 2020 Kuei-chun Chen. All rights reserved.
package demo;

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

import java.io.EOFException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
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
	private AtomicLong inserted;

	public Couch(Properties prop) {
		couchdbURI = prop.getProperty("couchdb.uri");
		mongodbURI = prop.getProperty("mongodb.uri");
		timeout = Integer.valueOf(prop.getProperty("couchdb.timeout"));
		numThreads = Integer.valueOf(prop.getProperty("num_threads"));
		couchBatchSize = Integer.valueOf(prop.getProperty("couch_batch_size"));
		mongoBatchSize = Integer.valueOf(prop.getProperty("mongo_batch_size"));
		dbName = prop.getProperty("database_name");
		collectionName = prop.getProperty("collection_name");

		fetched = new AtomicLong(0);
		inserted = new AtomicLong(0);
	}

	public void migrate() throws MalformedURLException, EOFException, InterruptedException {
		long startTime = System.currentTimeMillis();

		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThreads);
		HttpClient httpClient = new StdHttpClient.Builder().url(couchdbURI).connectionTimeout(timeout)
				.socketTimeout(timeout).build();
		CouchDbInstance dbInstance = new StdCouchDbInstance(httpClient);
		CouchDbConnector db = dbInstance.createConnector(dbName, true);

		try (Mongo mongo = new Mongo(this.mongodbURI)) {
			long count = mongo.countDocuments(dbName, collectionName);
			inserted.getAndSet(count) ;

			// Record
			long startTime2 = System.currentTimeMillis();

			// Get all documents
			ViewQuery query = new ViewQuery().allDocs().includeDocs(false);
			ViewResult result = db.queryView(query);

			logger.debug(String.format("migrate() spent %d mills while running initial query", System.currentTimeMillis() - startTime2));

			if (result.isEmpty()) {
				throw new EOFException("no document found");
			}

			logger.info(String.format("found %d documents in CouchDB", result.getSize()));
			int counter = 0;
			String startDocId = null;
			String endDocId = null;

			// Divide into batches
			for (ViewResult.Row row : result.getRows()) {
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

						ViewQuery q = new ViewQuery().allDocs().includeDocs(true).startDocId(startDocumentId).endDocId(endDocumentId)
								.inclusiveEnd(true);
						ViewResult res = db.queryView(q);
						logger.debug(String.format("migrate() spent %d millis running query to retrieve docs between start id %s and end id %s from couchbase",
								System.currentTimeMillis() - startTime3,
								startDocumentId,
								endDocumentId));

						// Record
						startTime3 = System.currentTimeMillis();
						fetched.addAndGet(res.getSize());

						logger.debug(String.format("migrate() spent %d millis tabulating couchbase result size", System.currentTimeMillis() - startTime3));

						long id = Thread.currentThread().getId() % numThreads;
						logger.info(String.format("Starting thread with id %d", id));

						processViewResults(res, mongo, id);

						logger.info(String.format("Thread %d finished ", id));
					});

					logger.info(String.format("fetching, total of %d fetched, %d inserted", fetched.get(), inserted.get()));
					counter = 0;
					while (executor.getQueue().size() > numThreads) {
						logger.info(String.format("thread has %d jobs in queue, throttling", executor.getQueue().size()));
						Thread.sleep(5000);	// throttle and yield
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
					ViewResult res = db.queryView(q);
					logger.debug(String.format("migrate() spent %d millis running query to retrieve docs between start id %s and end id %s from couchbase",
							System.currentTimeMillis() - startTime3,
							startDocumentId,
							endDocumentId));

					startTime3 = System.currentTimeMillis();
					fetched.addAndGet(res.getSize());
					logger.debug(String.format("migrate() spent %d millis tabulating couchbase result size", System.currentTimeMillis() - startTime3));

					long id = Thread.currentThread().getId() % numThreads;
					logger.info(String.format("Starting thread with id %d", id));

					processViewResults(res, mongo, id);

					logger.info(String.format("Thread %d finished ", id));
				});
			}

			logger.info(String.format("end of fetching, total of %d fetched, %d inserted", fetched.get(), inserted.get()));
			logger.info(String.format("last batch start key: %s, end key: %s", startDocId, endDocId));
			long inMongo = mongo.countDocuments(dbName, collectionName);
			while (fetched.get() != inMongo) {

				logger.info(String.format("total of %d fetched, %d in mongo", fetched.get(), inMongo));
				Thread.sleep(5000);
				inMongo = mongo.countDocuments(dbName, collectionName);
			}
			executor.shutdown();
			executor.awaitTermination(5, TimeUnit.SECONDS);
			inMongo = mongo.countDocuments(dbName, collectionName);
			logger.info(String.format("total of %d fetched, %d in mongo", fetched.get(), inMongo));

			logger.debug(String.format("migrate() spent %d millis total migrating %d documents", System.currentTimeMillis() - startTime, inMongo));

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	private void processViewResults(ViewResult result, Mongo mongo, long id) {
		logger.debug(String.format("Started processViewResults on thread %d and %d results", id, result.getSize()));

		long startTime = System.currentTimeMillis();

		List<Document> documents = new ArrayList<>();
		int i = 0;
		for (ViewResult.Row row : result.getRows()) {
			documents.add(Document.parse(row.getDoc()));
			if ((++i) % mongoBatchSize == 0) {
				int saved = mongo.saveToMongo(this.dbName, this.collectionName, documents);
				String message = String.format("[%d] %d sent, %d inserted to mongo", id, documents.size(), saved);
				logger.info(message);

				inserted.addAndGet(saved);
				documents = new ArrayList<>();
			}
		}
		if (!documents.isEmpty()) {
			int saved = mongo.saveToMongo(this.dbName, this.collectionName, documents);
			String message = String.format("[%d] %d sent, %d inserted to mongo", id, documents.size(), saved);
			logger.info(message);

			inserted.addAndGet(saved);
		}

		logger.debug(String.format("Returning from processViewResults on thread %d", id));

		// Record time
		long endTime = System.currentTimeMillis();
		logger.debug(String.format("processViewResults() on id %d lasted %d mills", id, endTime - startTime));

	}
}
