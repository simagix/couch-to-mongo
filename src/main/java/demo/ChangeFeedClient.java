package demo;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.bson.Document;
import org.ektorp.CouchDbConnector;
import org.ektorp.changes.ChangesCommand;
import org.ektorp.changes.DocumentChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChangeFeedClient {

	private static Logger logger = LoggerFactory.getLogger(ChangeFeedClient.class);

    public static final String NO_SEQUENCE_NUMBER_FOUND = "NO_SEQUENCE_NUMBER_FOUND";

    private UUID sessionId;
    private String lastSequenceNumber;
    private Mongo mongo;
    private CouchDbConnector couchDB;
    private String dbName;
    private String collectionName;
    private int batchSize = 1000;

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

    public void setBatchSize(int batchSize) {
    	if (batchSize > 0) {
    		this.batchSize = batchSize;
    	}
    }

    public void applyChanges() {

        logger.info("Beginning to apply changes...");
        mongo.insertMetaDataOperation("beginApplyChanges", new Date());

        final int NUM_SECS = 5;

        Thread logLastSequenceNumShutdownHook = new Thread(() -> logLastSequenceNumber());
        Runtime.getRuntime().addShutdownHook(logLastSequenceNumShutdownHook);

        try {
            while (true) {

                logger.debug("Checking change feed for latest changes...");
                Set<Document> changedDocuments = getChangedDocumentsFromChangeFeed();
                insertDataIntoMongo(changedDocuments);

                logLastSequenceNumber();

                if (changedDocuments.size() < batchSize) {
	                logger.info(String.format("All caught up. Waiting %ds for more changes. Terminate at any time", NUM_SECS));
	                Thread.sleep(NUM_SECS*1000);
                }
            }
        } catch (InterruptedException ex ) {
            logger.error(String.format("Encountered exception when sleeping for %d millis: " + ex, NUM_SECS*1000));
            logger.error(ex.getMessage());
        }
    }

//    private Document getDocumentForId(String id) {
//        ViewQuery q = new ViewQuery().allDocs().includeDocs(true).startDocId(id).endDocId(id).inclusiveEnd(true);
//        ViewResult result = couchDB.queryView(q);
//
//        for (ViewResult.Row row : result.getRows()) {
//            return Document.parse(row.getDoc());
//        }
//        return null;
//    }

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
    private void insertDataIntoMongo(Set<Document> changedDocuments) {

        logger.debug(String.format("Processing %d changes", changedDocuments.size()));

        if (changedDocuments.size() > 0) {
            long threadId = Thread.currentThread().getId();
        	 mongo.updateDocsInMongo(dbName, collectionName, changedDocuments, threadId);
        }
    }

    private Set<Document> getChangedDocumentsFromChangeFeed() {

        ChangesCommand changesCommand = new ChangesCommand.Builder().since(lastSequenceNumber)
                .includeDocs(true)
                .limit(batchSize)
                .build();

        Set<Document> changedDocuments = new HashSet<>();

        try {
            List<DocumentChange> changeFeed = couchDB.changes(changesCommand);
            logger.info("fetched {} changed documents from cloudant for sequence number {}",
            		changeFeed.size(), lastSequenceNumber);

            // Coalesce updates per document via a map
            for (DocumentChange change : changeFeed) {

                String docId = change.getId();

                Document changedDocument = Document.parse(change.getDoc());
                changedDocuments.add(changedDocument);

                String sequenceNum = getChangeSequenceNumber(change);
                if (NO_SEQUENCE_NUMBER_FOUND.equals(sequenceNum)) {
                    logger.error("Could not find a sequence number for document with id " + docId);
                    continue;
                }
                lastSequenceNumber = sequenceNum.replace("\"","");

                logger.debug("Adding change for id " + docId);
            }
        } catch(Exception ex) {
            logger.error("encountered exception", ex);
        }
        return changedDocuments;
    }

    /***
     *
     * @param change
     * @return
     */
    private String getChangeSequenceNumber(DocumentChange change) {
        logger.debug("Attempting to find sequence number for a document change");

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

        logger.warn("Could not find any sequence number for this document change");
        return NO_SEQUENCE_NUMBER_FOUND;
    }
}
