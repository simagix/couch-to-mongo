// Copyright 2020 Kuei-chun Chen. All rights reserved.
package demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

@SpringBootApplication
public class CouchToMongo {
	private static Logger logger = LoggerFactory.getLogger(CouchToMongo.class);

	public static void main(String[] args) {
		SpringApplication.run(CouchToMongo.class, args);

		logger.info("Found args :" + Arrays.toString(args));

		try {
			String filename = "migration.properties";
			String lastSequenceNum = "20000-g1AAAACheJzLYWBgYMpgTmEQTM4vTc5ISXLIyU9OzMnILy7JAUklMiTV____PyuDOYmBQe1yLlCMPTklOSnFIgmbHjwm5bEASYYGIPUfbqC6L9jARIskY8M0I2xaswDQTTLG";
			if (args.length > 0) {
				filename = args[0];
				lastSequenceNum = args[1];
				logger.info("Setting filename to " + filename);
			}
			new demo.Couch(readProperties(filename)).migrate(lastSequenceNum);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static Properties readProperties(String filename) {
		Properties prop = new Properties();
		try (InputStream input = new FileInputStream(filename)) {
			logger.info("Attempting to load properties file " + filename);
			prop.load(input);
		} catch (IOException ex) {

			logger.error(String.format("Encountered an issue attempting to read from property file %s: %s", filename, ex.getMessage()));
			logger.error(ex.toString());

			logger.info("Using default properties after error...");
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
