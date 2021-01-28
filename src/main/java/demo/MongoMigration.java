// Copyright 2020 Kuei-chun Chen. All rights reserved.
package demo;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.result.InsertManyResult;

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
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MongoMigration {
	private Logger logger = LoggerFactory.getLogger(MongoMigration.class);
	
	private String couchdbURI = "http://127.0.0.1:5984";
	private String mongodbURI = "mongodb://user:password@localhost/?replicaSet=replset&authSource=admin";
	private String dbName = "demo";
	private String collectionName = "sample_docs";
	private int numThreads = 8;
	private int couchBatchSize = 10000;
	private int mongoBatchSize = 1000;
	private int fetched = 0;
	private int inserted = 0;
	private int offset = 0;

	public void execute() throws MalformedURLException, InterruptedException {
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThreads);
		HttpClient httpClient = new StdHttpClient.Builder().url(couchdbURI).build();
		CouchDbInstance dbInstance = new StdCouchDbInstance(httpClient);
		CouchDbConnector db = dbInstance.createConnector(dbName, true);

		ConnectionString connectionString = new ConnectionString(mongodbURI);
		MongoClientSettings clientSettings = MongoClientSettings.builder().applyConnectionString(connectionString).build();
		for (int t = 0; t < numThreads; t++) {
			executor.submit(() -> {
				long id = Thread.currentThread().getId() % numThreads;
				try (MongoClient mongoClient = MongoClients.create(clientSettings)) {
					MongoCollection<Document> collection = mongoClient.getDatabase(dbName).getCollection(collectionName);

					for (;;) {
						int ptr;
						synchronized (this) {
							ptr = this.offset;
							this.offset += couchBatchSize;
						}
						logger.debug(String.format("[%d] skipped %d, limited: %d", id, ptr, couchBatchSize));
						ViewQuery query = new ViewQuery().allDocs().includeDocs(true).limit(couchBatchSize).skip(ptr);
						ViewResult result = db.queryView(query);
						if (result.isEmpty()) {
							logger.debug(String.format("[%d] exiting loop", id));
							break;
						}
						synchronized (this) {
							this.fetched += result.getSize();
							logger.info(String.format("[%d] total of %d fetched", id, this.fetched));
						}

						processViewResults(result, collection, id);
						Thread.sleep(100);
					}
				}
				logger.debug(String.format("[%d] returning", id));
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

	public void readProperties(String filename) {
		try (InputStream input = new FileInputStream(filename)) {
			Properties prop = new Properties();
			prop.load(input);
			couchdbURI = prop.getProperty("couchdb.uri");
			mongodbURI = prop.getProperty("mongodb.uri");
			numThreads = Integer.valueOf(prop.getProperty("num_threads"));
			couchBatchSize = Integer.valueOf(prop.getProperty("couch_batch_size"));
			mongoBatchSize = Integer.valueOf(prop.getProperty("mongo_batch_size"));
			dbName = prop.getProperty("database_name");
			collectionName = prop.getProperty("collection_name");
		} catch (IOException ex) {
			logger.debug("use default properties");
		}
	}

	private void processViewResults(ViewResult result, MongoCollection<Document> collection, long id) {
		List<Document> documents = new ArrayList<>();
		int i = 0;
		for (ViewResult.Row row : result.getRows()) {
			documents.add(Document.parse(row.getDoc()));
			if ((++i) % mongoBatchSize == 0) {
				saveToMongo(collection,  documents, id);
				documents = new ArrayList<>();
			}
		}
		saveToMongo(collection, documents, id);
	}

	private void saveToMongo(MongoCollection<Document> collection, List<Document> documents, long id) {
		if (documents.isEmpty()) {
			return;
		}
		InsertManyOptions options = new InsertManyOptions();
		options.ordered(false);
		String message = String.format("documents size %d", documents.size());
		logger.debug(message);
		InsertManyResult res = collection.insertMany(documents, options);
		synchronized (this) {
			this.inserted += res.getInsertedIds().size();
			message = String.format("[%d] %d sent, %d inserted", id, documents.size(), res.getInsertedIds().size());
			logger.debug(message);
		}
	}

	public static void main(String[] args) {
		SpringApplication.run(MongoMigration.class, args);
		MongoMigration migration = new MongoMigration();
		try {
			migration.readProperties("migration.properties");
			migration.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
