package demo;

import org.bson.Document;
import org.ektorp.CouchDbConnector;
import org.ektorp.ViewQuery;
import org.ektorp.ViewResult;
import org.ektorp.changes.ChangesCommand;
import org.ektorp.changes.ChangesFeed;
import org.ektorp.changes.DocumentChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;

import javax.print.Doc;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class ChangeFeedClient {

    private static Logger logger = LoggerFactory.getLogger(ChangeFeedClient.class);

    private final String NO_SEQUENCE_NUMBER_FOUND = "NO_SEQUENCE_NUMBER_FOUND";
    private final int BATCH_SIZE = 100;

    private UUID sessionId;
    private String lastSequenceNumber;
    private Mongo mongo;
    private CouchDbConnector couchDB;
    private String dbName;
    private String collectionName;

    public ChangeFeedClient(String lastSequenceNumber, Mongo mongo, String dbName, String collectionName, CouchDbConnector couchDB) {

        this.mongo = mongo;
        this.sessionId = mongo.getSessionId();
        this.lastSequenceNumber = lastSequenceNumber;
        if (lastSequenceNumber == null) {
            this.lastSequenceNumber = mongo.getLastSequenceNumber();
            logger.info(String.format("Last Sequence number is null, fetched new last sequence number of %s from MongoDB", lastSequenceNumber));
        } else {
            logger.debug(String.format("Inserting last sequence number of %s in MongoDB", lastSequenceNumber));
            mongo.insertLastSequenceNumber(lastSequenceNumber, new Date());
        }

        this.dbName = dbName;
        this.collectionName = collectionName;
        this.couchDB = couchDB;
    }

    public void applyChanges() {

        logger.info("Beginning to apply changes...");
        mongo.insertMetaDataOperation("beginApplyChanges", new Date());

        final int NUM_SECS = 5;

        Thread logLastSequenceNumShutdownHook = new Thread(() -> logLastSequenceNumber());
        Runtime.getRuntime().addShutdownHook(logLastSequenceNumShutdownHook);

        try {
            while (true) {

                logger.info("Checking change feed for latest changes...");
                Set<String> changedIds = getChangeIdsFromChangeFeed();
                insertDataIntoMongo(changedIds);

                logLastSequenceNumber();

                logger.info(String.format("All caught up. Waiting %ds for more changes. Terminate at any time", NUM_SECS));
                Thread.sleep(NUM_SECS*1000);
            }
        } catch (InterruptedException ex ) {
            logger.error(String.format("Encountered exception when sleeping for %d millis: " + ex, NUM_SECS*1000));
            logger.error(ex.getMessage());
        }
    }

    private Document getDocumentForId(String id) {
        ViewQuery q = new ViewQuery().allDocs().includeDocs(true).startDocId(id).endDocId(id).inclusiveEnd(true);
        ViewResult result = couchDB.queryView(q);

        for (ViewResult.Row row : result.getRows()) {
            return Document.parse(row.getDoc());
        }
        return null;
    }

    private void logLastSequenceNumber() {
        mongo.insertLastSequenceNumber(lastSequenceNumber, new Date());
    }

    /***
     * Insert Data Into Mongo
     *
     *
     *
     * @param changedIds
     */
    private void insertDataIntoMongo(Set<String> changedIds) {
        logger.info(String.format("Processing %d changes", changedIds.size()));
        long threadId = Thread.currentThread().getId();

        Set<Document>  changedDocs = new HashSet<>();
        for (String changeDocId : changedIds) {

            ViewQuery q = new ViewQuery().allDocs().includeDocs(true).startDocId(changeDocId).endDocId(changeDocId)
                                                    .inclusiveEnd(true);
            ViewResult result = couchDB.queryView(q);
            for (ViewResult.Row row : result.getRows()) {
                Document document = Document.parse(row.getDoc());
                logger.debug("Adding doc with id " + document.get("_id").toString() + ": " + row.getDoc());
                changedDocs.add(document);
            }

            if (changedDocs.size() % BATCH_SIZE == 0) {
                logger.debug(String.format("Processing batch of %d updates/inserts", changedDocs.size()));
                mongo.updateDocsInMongo(dbName, collectionName, changedDocs, threadId);
                changedDocs = new HashSet<>();
            }
        }

        // Process remaining documents
        if (changedDocs.size() > 0 ) {
            logger.debug(String.format("Processing batch of %d upodates/inserts", changedDocs.size()));
            mongo.updateDocsInMongo(dbName, collectionName, changedDocs, threadId);
        }
    }

    private Set<String> getChangeIdsFromChangeFeed() {
        ChangesCommand changesCommand = new ChangesCommand.Builder().since(lastSequenceNumber)
                .includeDocs(true)
                .build();
        List<DocumentChange> changeFeed = couchDB.changes(changesCommand);

        // Coalesce updates per document via a map
        Set<String> changedIds = new HashSet<>();
        for (DocumentChange change : changeFeed) {

            String docId = change.getId();

            String sequenceNum = getSequenceNumber(change);
            if (NO_SEQUENCE_NUMBER_FOUND.equals(sequenceNum)) {
                logger.error("Could not find a sequence number for document with id " + docId);
                continue;
            }
            lastSequenceNumber = sequenceNum.replace("\"","");

            logger.debug("Adding change for id " + docId);
            changedIds.add(docId);
        }
        return changedIds;
    }

    /***
     *
     * @param change
     * @return
     */
    private String getSequenceNumber(DocumentChange change) {
        logger.info("Attempting to find sequence number for a document change");

        String[] changeParts = change.toString().split("\\{");
        changeParts = changeParts[1].split("\\}");

        changeParts = changeParts[0].split(",");
        for (String changePart : changeParts) {
            logger.trace("Attempting to analyze change part " + changePart);
            String[] changePartParts = changePart.split(":");
            String key = changePartParts[0];
            if ("\"seq\"".equals(key)) {
                String value = changePartParts[1];
                return value;
            }
        }

        logger.debug("Could not find any sequence number for this document change");
        return NO_SEQUENCE_NUMBER_FOUND;
    }

//    public static void main(String[] args) {
//        SpringApplication.run(CouchToMongo.class, args);
//
//        logger.info("Found args :" + Arrays.toString(args));
//
//        try {
//            String filename = "migration.properties";
//            if (args.length > 0) {
//                filename = args[0];
//                logger.info("Setting filename to " + filename);
//            }
//            new demo.Couch(readProperties(filename)).migrate();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
}
