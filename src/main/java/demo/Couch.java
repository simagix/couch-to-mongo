// Copyright 2020 Kuei-chun Chen. All rights reserved.
package demo;

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
	private String couchdbUsername;
	private String couchdbPassword;
	private int timeout;
    private String mongodbURI;
	private String dbName;
	private String collectionName;
	private int numThreads;
	private int couchBatchSize;
    private int mongoBatchSize;
    
	private int fetched = 0;
	private int inserted = 0;
    private int offset = 0;
    
    public Couch(Properties prop) {
        couchdbURI = prop.getProperty("couchdb.uri");
        mongodbURI = prop.getProperty("mongodb.uri");
        couchdbUsername = prop.getProperty("couchdb.username");
        couchdbPassword = prop.getProperty("couchdb.password");
        timeout = Integer.valueOf(prop.getProperty("couchdb.timeout"));
        numThreads = Integer.valueOf(prop.getProperty("num_threads"));
        couchBatchSize = Integer.valueOf(prop.getProperty("couch_batch_size"));
        mongoBatchSize = Integer.valueOf(prop.getProperty("mongo_batch_size"));
        dbName = prop.getProperty("database_name");
        collectionName = prop.getProperty("collection_name");
    }

	public void migrate() throws MalformedURLException, InterruptedException {
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThreads);
		HttpClient httpClient = new StdHttpClient.Builder().url(couchdbURI)
			.connectionTimeout(timeout).socketTimeout(timeout)
			.username(couchdbUsername).password(couchdbPassword)
			.build();
		CouchDbInstance dbInstance = new StdCouchDbInstance(httpClient);
		CouchDbConnector db = dbInstance.createConnector(dbName, true);

		for (int t = 0; t < numThreads; t++) {
			executor.submit(() -> {
				long id = Thread.currentThread().getId() % numThreads;
				try (Mongo mongo = new Mongo(this.mongodbURI)) {
					for (;;) {
						int ptr;
						synchronized (this) {
							ptr = this.offset;
							this.offset += couchBatchSize;
						}
						try {
							logger.debug(String.format("[%d] skipped %d, limited: %d", id, ptr, couchBatchSize));
							ViewQuery query = new ViewQuery().allDocs().includeDocs(true).limit(couchBatchSize).skip(ptr);
							ViewResult result = db.queryView(query);
							if (result.isEmpty()) {
								logger.info(String.format("[%d] exiting loop, %s.%s has %d documents", id, dbName, collectionName, mongo.countDocuments(dbName, collectionName)));
								break;
							}
							synchronized (this) {
								this.fetched += result.getSize();
								logger.info(String.format("[%d] total of %d fetched", id, this.fetched));
							}

							processViewResults(result, mongo, id);
						} catch(Exception ex) {
							logger.error(ex.getMessage());
						}
						Thread.sleep(100);
					}
				}
				return null;
			});
		}
		int count = 0;
		while(count++ < 10 && this.fetched != this.inserted) {
			String message = String.format("fetched %d, inserted %d", this.fetched, this.inserted);
			logger.debug(message);
			Thread.sleep(5000);
        }
		executor.shutdown();
		executor.awaitTermination(5, TimeUnit.SECONDS);
    }
    
	private void processViewResults(ViewResult result, Mongo mongo, long id) {
		List<Document> documents = new ArrayList<>();
		int i = 0;
		for (ViewResult.Row row : result.getRows()) {
			documents.add(Document.parse(row.getDoc()));
			if ((++i) % mongoBatchSize == 0) {
				int saved = mongo.saveToMongo(this.dbName, this.collectionName, documents);
				String message = String.format("[%d] %d sent, %d inserted", id, documents.size(), saved);
				logger.debug(message);
				synchronized (this) {
					this.inserted += saved;
				}
				documents = new ArrayList<>();
			}
		}
		if ( ! documents.isEmpty() ) {
			int saved = mongo.saveToMongo(this.dbName, this.collectionName, documents);
			String message = String.format("[%d] %d sent, %d inserted", id, documents.size(), saved);
			logger.debug(message);
			synchronized (this) {
				this.inserted += saved;
			}
		}
	}
}
