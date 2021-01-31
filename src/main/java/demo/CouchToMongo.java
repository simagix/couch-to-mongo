// Copyright 2020 Kuei-chun Chen. All rights reserved.
package demo;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class CouchToMongo {
	private static Logger logger = LoggerFactory.getLogger(CouchToMongo.class);

	public static void main(String[] args) {
		SpringApplication.run(CouchToMongo.class, args);
		
		try {
			String filename = "migration.properties";
			if (args.length > 1) {
				filename = args[1];
			}
			new Couch(readProperties(filename)).migrate();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static Properties readProperties(String filename) {
		Properties prop = new Properties();
		try (InputStream input = new FileInputStream(filename)) {
			prop.load(input);
		} catch (IOException ex) {
			logger.info("use default properties");
			prop.setProperty("couchdb.uri", "http://user:password@127.0.0.1:5984");
			prop.setProperty("couchdb.timeout", String.valueOf(60000));
			prop.setProperty("mongodb.uri", "mongodb://user:password@localhost/?replicaSet=replset&authSource=admin");
			prop.setProperty("num_threads", String.valueOf(4));
			prop.setProperty("couch_batch_size", String.valueOf(10000));
			prop.setProperty("mongo_batch_size", String.valueOf(1000));
			prop.setProperty("database_name", "demo");
			prop.setProperty("collection_name", "sample_docs");
		}
		return prop;
	}
}
