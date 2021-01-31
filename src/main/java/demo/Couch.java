// Copyright 2020 Kuei-chun Chen. All rights reserved.
package demo;

import java.io.EOFException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
	private String couchdbURI;
	private int timeout;
	private String mongodbURI;
	private String dbName;
	private String collectionName;
	private int numThreads;
	private int couchBatchSize;
	private int mongoBatchSize;

	private long fetched = 0;
	private long inserted = 0;

	public Couch(Properties prop) {
		couchdbURI = prop.getProperty("couchdb.uri");
		mongodbURI = prop.getProperty("mongodb.uri");
		timeout = Integer.valueOf(prop.getProperty("couchdb.timeout"));
		numThreads = Integer.valueOf(prop.getProperty("num_threads"));
		couchBatchSize = Integer.valueOf(prop.getProperty("couch_batch_size"));
		mongoBatchSize = Integer.valueOf(prop.getProperty("mongo_batch_size"));
		dbName = prop.getProperty("database_name");
		collectionName = prop.getProperty("collection_name");
	}

	public void migrate() throws MalformedURLException, EOFException, InterruptedException {
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThreads);
		HttpClient httpClient = new StdHttpClient.Builder().url(couchdbURI).connectionTimeout(timeout)
				.socketTimeout(timeout).build();
		CouchDbInstance dbInstance = new StdCouchDbInstance(httpClient);
		CouchDbConnector db = dbInstance.createConnector(dbName, true);

		try (Mongo mongo = new Mongo(this.mongodbURI)) {
			this.inserted = mongo.countDocuments(dbName, collectionName);
			ViewQuery query = new ViewQuery().allDocs().includeDocs(false);
			ViewResult result = db.queryView(query);
			if (result.isEmpty()) {
				throw new EOFException("no document found");
			}
			logger.info(String.format("found %d documents in CouchDB", result.getSize()));
			int counter = 0;
			String startDocId = null;
			String endDocId = null;
			for (ViewResult.Row row : result.getRows()) {
				counter++;
				endDocId = row.getId();
				if (counter == 1) {
					startDocId = row.getId();
					counter++;
				} else if (counter == couchBatchSize) {
					ViewQuery q = new ViewQuery().allDocs().includeDocs(true).startDocId(startDocId).endDocId(endDocId)
							.inclusiveEnd(true);
					ViewResult res = db.queryView(q);
					this.fetched += res.getSize();
					executor.submit(() -> {
						long id = Thread.currentThread().getId() % numThreads;
						processViewResults(res, mongo, id);
					});
					logger.info(String.format("fetching, total of %d fetched, %d inserted", this.fetched, this.inserted));
					counter = 0;
					while (executor.getQueue().size() > numThreads) {
						logger.info(String.format("thread has %d jobs in queue, throttling", executor.getQueue().size()));
						Thread.sleep(5000);	// throttle and yield
					}
				}
			}
			if (counter > 0) {
				ViewQuery q = new ViewQuery().allDocs().includeDocs(true).startDocId(startDocId).endDocId(endDocId)
						.inclusiveEnd(true);
				ViewResult res = db.queryView(q);
				this.fetched += res.getSize();
				executor.submit(() -> {
					long id = Thread.currentThread().getId() % numThreads;
					processViewResults(res, mongo, id);
				});
			}
			logger.info(String.format("end of fetching, total of %d fetched, %d inserted", this.fetched, this.inserted));
			logger.info(String.format("last batch start key: %s, end key: %s", startDocId, endDocId));
			long inMongo = mongo.countDocuments(dbName, collectionName);
			while (this.fetched != inMongo) {
				synchronized (this) {
					logger.info(String.format("total of %d fetched, %d in mongo", this.fetched, inMongo));
				}
				Thread.sleep(5000);
				inMongo = mongo.countDocuments(dbName, collectionName);
			}
			executor.shutdown();
			executor.awaitTermination(5, TimeUnit.SECONDS);
			inMongo = mongo.countDocuments(dbName, collectionName);
			logger.info(String.format("total of %d fetched, %d in mongo", this.fetched, inMongo));
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	private void processViewResults(ViewResult result, Mongo mongo, long id) {
		List<Document> documents = new ArrayList<>();
		int i = 0;
		for (ViewResult.Row row : result.getRows()) {
			documents.add(Document.parse(row.getDoc()));
			if ((++i) % mongoBatchSize == 0) {
				int saved = mongo.saveToMongo(this.dbName, this.collectionName, documents);
				String message = String.format("[%d] %d sent, %d inserted to mongo", id, documents.size(), saved);
				logger.debug(message);
				synchronized (this) {
					this.inserted += saved;
				}
				documents = new ArrayList<>();
			}
		}
		if (!documents.isEmpty()) {
			int saved = mongo.saveToMongo(this.dbName, this.collectionName, documents);
			String message = String.format("[%d] %d sent, %d inserted to mongo", id, documents.size(), saved);
			logger.debug(message);
			synchronized (this) {
				this.inserted += saved;
			}
		}
	}
}
