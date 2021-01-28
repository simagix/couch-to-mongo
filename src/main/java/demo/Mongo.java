// Copyright 2020 Kuei-chun Chen. All rights reserved.
package demo;

import java.util.List;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.result.InsertManyResult;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Mongo implements AutoCloseable {
    private Logger logger = LoggerFactory.getLogger(Mongo.class);
    private MongoClient mongoClient;

    public Mongo(String mongodbURI) {
        ConnectionString connectionString = new ConnectionString(mongodbURI);
        MongoClientSettings clientSettings = MongoClientSettings.builder().applyConnectionString(connectionString)
                .build();
        mongoClient = MongoClients.create(clientSettings);
    }

    @Override
    public void close() throws Exception {
        mongoClient.close();
    }

	public int saveToMongo(String dbName, String collectionName, List<Document> documents) {
		if (documents.isEmpty()) {
			return 0;
        }
        MongoCollection<Document> collection = mongoClient.getDatabase(dbName).getCollection(collectionName);
		InsertManyOptions options = new InsertManyOptions();
		options.ordered(false);
		String message = String.format("documents size %d", documents.size());
		logger.debug(message);
		InsertManyResult res = collection.insertMany(documents, options);
        return res.getInsertedIds().size();
    }
    
    public long countDocuments(String dbName, String collectionName) {
        MongoCollection<Document> collection = mongoClient.getDatabase(dbName).getCollection(collectionName);
        return collection.countDocuments();
    }
}
