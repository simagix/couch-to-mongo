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
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MongoMigration {
	private String couchdbURI = "http://127.0.0.1:5984";
	private String mongodbURI = "mongodb://user:password@localhost/?replicaSet=replset&authSource=admin";
	private int numThreads = 8;
	private int couchBatchSize = 10000;
	private int mongoBatchSize = 1000;
	private String dbName = "demo";
	private String collectionName = "sample_docs";

	public void execute() throws MalformedURLException, InterruptedException {
        ConnectionString connectionString = new ConnectionString(mongodbURI);
        MongoClientSettings clientSettings = MongoClientSettings.builder().applyConnectionString(connectionString).build();
		try (MongoClient mongoClient = MongoClients.create(clientSettings)) {
			MongoCollection<Document> collection = mongoClient.getDatabase(dbName).getCollection(collectionName);
			ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numThreads);
			HttpClient httpClient = new StdHttpClient.Builder().url(couchdbURI).build();
			CouchDbInstance dbInstance = new StdCouchDbInstance(httpClient);
			CouchDbConnector db = dbInstance.createConnector(dbName, true);
			int total = 0;
			for(;;) {
				System.out.printf("skipped %d, limited: %d%n", total, couchBatchSize);
				ViewQuery query = new ViewQuery().allDocs().includeDocs(true).limit(couchBatchSize).skip(total);
				ViewResult list = db.queryView(query);
				if (list.isEmpty()) {
					break;
				}
				executor.submit(() -> {
					InsertManyOptions options = new InsertManyOptions();
					options.ordered(false);
					List<Document> documents = new ArrayList<>();
					int i = 0;
					for (ViewResult.Row row : list.getRows()) {
						documents.add(Document.parse(row.getDoc()));
						if ( ++i % mongoBatchSize == 0) { 
							InsertManyResult res = collection.insertMany(documents, options);
							System.out.printf("%d sent, %d inserted%n", documents.size(), res.getInsertedIds().size());
							documents = new ArrayList<>();
						}
					}
					if (! documents.isEmpty()) {
						InsertManyResult res = collection.insertMany(documents, options);
						System.out.printf("%d sent, %d inserted%n", documents.size(), res.getInsertedIds().size());
					}
					Thread.sleep(100);
					return null;
				});
				total += list.getSize();
			}
	
			executor.shutdown();
			executor.awaitTermination(5, TimeUnit.SECONDS);
		}
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
			System.out.println("use default properties");
		}
	}

	public static void main(String[] args) {    
		SpringApplication.run(MongoMigration.class, args);
		MongoMigration migration = new MongoMigration();
		try {
			migration.readProperties("migration.properties");
			migration.execute();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
