package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.datamovement.DataMovementManager;
import com.marklogic.client.datamovement.WriteBatcher;
import com.marklogic.client.dataservices.IOEndpoint;
import com.marklogic.client.dataservices.InputCaller;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.JacksonHandle;
import com.marklogic.xcc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Each test method in this class follows an approach of first creating all of the objects to be written to ML, and then
 * writing all of those objects. Only the second part is timed for performance; this ensures that any time spent
 * creating objects via Java code doesn't count against the performance of actually writing the objects to ML.
 */
public class PerformanceTester {

    private final static Logger logger = LoggerFactory.getLogger(PerformanceTester.class);

    // Initial default values; override these args
    private static int batchCount = 100;
    private static int batchSize = 100;
    private static int iterations = 3;
    private static int threadCount = 32;
    private static String host = "localhost";
    private static int restPort = 8003;
    private static int xdbcPort = 8004;
    private static String username = "admin";
    private static String password = "admin";

    private static final String COLLECTION = "data";

    private static DatabaseClient databaseClient;
    private static ContentSource contentSource;

    private static Map<String, List<Long>> durationsMap = new LinkedHashMap();

    /**
     * Localhost:
     * XCC 8003: [3265, 2454, 2625, 2902, 2948], [2833, 2538, 2493, 2320, 2356]
     * XCC 8004: [2900, 2455, 2341, 2542, 2722], [2755, 2386, 2230, 2243, 2399]
     * <p>
     * XCC: [3044, 2477, 2599, 2551, 2454]
     * DMSDK: [2471, 1838, 1800, 1689, 3007]
     * Bulk: [1185, 911, 824, 876, 2016]
     * <p>
     * Remote:
     * XCC: [7866, 7600, 9750, 7596, 7628]
     * DMSDK: [4231, 900, 890, 900, 900]
     * Bulk: [366, 349, 379, 375, 368]
     *
     * @param args
     */
    public static void main(String[] args) {
        applyArgs(args);

        logger.info(String.format("Connecting to '%s' as user '%s'", host, username));
        logger.info("Batch count: " + batchCount);
        logger.info("Batch size: " + batchSize);
        logger.info("Thread count: " + threadCount);
        logger.info("Iterations: " + iterations);

        contentSource = ContentSourceFactory.newContentSource(host, xdbcPort, username, password.toCharArray());
        databaseClient = DatabaseClientFactory.newClient(host, restPort, new DatabaseClientFactory.DigestAuthContext(username, password));

        for (int i = 1; i <= iterations; i++) {
            testXcc();
            testWriteBatcher();
            testBulkInputCaller();
        }

        printDurations();
    }

    /**
     * Override the defaults values based on the command line arguments.
     *
     * @param args
     */
    private static void applyArgs(String[] args) {
        for (String arg : args) {
            if (arg.contains("=")) {
                String[] tokens = arg.split("=");
                switch (tokens[0]) {
                    case "host":
                        host = tokens[1];
                        break;
                    case "restPort":
                        restPort = Integer.parseInt(tokens[1]);
                        break;
                    case "xdbcPort":
                        xdbcPort = Integer.parseInt(tokens[1]);
                        break;
                    case "username":
                        username = tokens[1];
                        break;
                    case "password":
                        password = tokens[1];
                        break;
                    case "batchCount":
                        batchCount = Integer.parseInt(tokens[1]);
                        break;
                    case "batchSize":
                        batchSize = Integer.parseInt(tokens[1]);
                        break;
                    case "threadCount":
                        threadCount = Integer.parseInt(tokens[1]);
                        break;
                    case "iterations":
                        iterations = Integer.parseInt(tokens[1]);
                        break;
                }
            }
        }
    }

    /**
     * Before a test is run, delete any documents in the collection that documents will be written to.
     */
    private static void deleteData() {
        //logger.info(String.format("Deleting documents in collection '%s'", COLLECTION));
        databaseClient.newServerEval().xquery(String.format("xdmp:collection-delete('%s')", COLLECTION)).evalAs(String.class);
    }

    /**
     * Uses a Java ExecutorService to support multi-threading in a fashion similar to that of DMSDK and the Bulk API.
     */
    private static void testXcc() {
        deleteData();

        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        List<Content[]> batches = buildContentBatches();

        long start = System.currentTimeMillis();
        batches.forEach(contentArray -> {
            executorService.execute(() -> {
                // XCC docs state that Session is not thread-safe and also not to bother with pooling Session objects
                // because they are so cheap to create - https://docs.marklogic.com/guide/xcc/concepts#id_55196
                Session session = contentSource.newSession();
                try {
                    session.insertContent(contentArray);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                } finally {
                    session.close();
                }
            });
        });

        waitForThreadsToFinish(executorService);
        addDuration("XCC", System.currentTimeMillis() - start);
    }

    /**
     * Build batches of XCC Content objects based on batchCount and batchSize.
     *
     * @return
     */
    private static List<Content[]> buildContentBatches() {
        List<Content[]> documents = new ArrayList<>();
        ContentCreateOptions options = new ContentCreateOptions();
        options.setFormatXml();
        options.setCollections(new String[]{COLLECTION});
        for (int i = 0; i < batchCount; i++) {
            Content[] docs = new Content[batchSize];
            for (int j = 0; j < batchSize; j++) {
                String uuid = UUID.randomUUID().toString();
                docs[j] = ContentFactory.newContent(uuid + ".xml", "<hello>world</hello>", options);
            }
            documents.add(docs);
        }
        return documents;
    }

    /**
     * Wait for the ExecutorService, used when testing XCC, to finish.
     */
    private static void waitForThreadsToFinish(ExecutorService executorService) {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.MINUTES)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Tests performance of writing data via a vanilla WriteBatcher.
     */
    private static void testWriteBatcher() {
        deleteData();

        DataMovementManager dataMovementManager = databaseClient.newDataMovementManager();
        WriteBatcher writeBatcher = dataMovementManager.newWriteBatcher()
                .withThreadCount(threadCount)
                .withBatchSize(batchSize);

        List<List<DocumentWriteOperation>> batches = buildDocumentBatches();
        long start = System.currentTimeMillis();
        batches.forEach(docs -> {
            writeBatcher.addAll(docs.stream());
        });

        writeBatcher.flushAndWait();
        addDuration("DMSDK", System.currentTimeMillis() - start);
        dataMovementManager.stopJob(writeBatcher);
    }

    /**
     * Tests performance of writing data via a vanilla BulkInputCaller.
     */
    private static void testBulkInputCaller() {
        deleteData();

        InputCaller<JsonNode> inputCaller;

        JacksonHandle apiHandle = databaseClient.newServerEval()
                .javascript("xdmp.eval('cts.doc(\"/writeDocuments.api\")', {}, {database: xdmp.database('java-tester-modules')})")
                .eval(new JacksonHandle());

        try {
            inputCaller = InputCaller.on(databaseClient, apiHandle, new JacksonHandle());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        IOEndpoint.CallContext[] callContexts = new IOEndpoint.CallContext[threadCount];
        for (int i = 0; i < threadCount; i++) {
            callContexts[i] = inputCaller.newCallContext();
        }
        InputCaller.BulkInputCaller<JsonNode> bulkInputCaller = inputCaller.bulkCaller(callContexts, threadCount);

        List<List<DocumentWriteOperation>> batches = buildDocumentBatches();
        List<List<ObjectNode>> jsonBatches = convertToJsonObjects(batches);

        long start = System.currentTimeMillis();
        jsonBatches.forEach(batch -> {
            batch.forEach(object -> bulkInputCaller.accept(object));
        });

        bulkInputCaller.awaitCompletion();
        addDuration("Bulk", System.currentTimeMillis() - start);
    }

    /**
     * Converts DocumentWriteOperations into JSON objects that can be passed to ML via a BulkInputCaller. This is
     * intended to demonstrate an example of where the client is able to specify some details like collections and
     * permissions, such that those need to be included along with each document that they describe. The DS endpoint is
     * expected to utilize this data when inserting the document.
     *
     * @param batches
     * @return
     */
    private static List<List<ObjectNode>> convertToJsonObjects(List<List<DocumentWriteOperation>> batches) {
        ObjectMapper objectMapper = new ObjectMapper();
        List<List<ObjectNode>> jsonBatches = new ArrayList();
        for (List<DocumentWriteOperation> batch : batches) {
            List<ObjectNode> objects = new ArrayList();
            jsonBatches.add(objects);
            for (DocumentWriteOperation op : batch) {
                JsonNode content = ((JacksonHandle) op.getContent()).get();

                // Demonstrates an example where the client provides more of the details by wrapping the content in a
                // JSON object that includes URI + metadata. The intent is to be a bit more realistic where a client
                // would specify things like collections/permissions, such that the DS endpoint can't just define those
                // itself.
                ObjectNode node = objectMapper.createObjectNode();
                node.put("uri", UUID.randomUUID() + ".json");
                node.set("content", content);

                // Including the metadata as a string of XML results in slower performance in the DS endpoint, which if
                // written in SJS, must do some fairly expensive xpath'ing to extract the data. So the metadata is instead
                // converted to JSON here.
                DocumentMetadataHandle metadata = (DocumentMetadataHandle) op.getMetadata();
                ArrayNode collections = node.putArray("collections");
                metadata.getCollections().forEach(coll -> collections.add(coll));
                ArrayNode permissions = node.putArray("permissions");
                metadata.getPermissions().forEach((roleName, capabilities) -> {
                    capabilities.forEach(capability -> permissions.addObject().put("roleName", roleName).put("capability", capability.name().toLowerCase()));
                });

                objects.add(node);
            }
        }
        return jsonBatches;
    }

    /**
     * Build batches of DocumentWriteOperation objects based on batchCount and batchSize.
     *
     * @return
     */
    private static List<List<DocumentWriteOperation>> buildDocumentBatches() {
        DocumentMetadataHandle metadata = new DocumentMetadataHandle()
                .withCollections(COLLECTION, COLLECTION + "2")
                .withPermission("rest-reader", DocumentMetadataHandle.Capability.READ, DocumentMetadataHandle.Capability.UPDATE);

        ObjectMapper objectMapper = new ObjectMapper();
        List<List<DocumentWriteOperation>> documents = new ArrayList<>();
        for (int i = 0; i < batchCount; i++) {
            List<DocumentWriteOperation> docs = new ArrayList<>();
            for (int j = 0; j < batchSize; j++) {
                ObjectNode content = objectMapper.createObjectNode().put("hello", "world");
                docs.add(new DocumentWriteOperationImpl(DocumentWriteOperation.OperationType.DOCUMENT_WRITE,
                        UUID.randomUUID() + ".json", metadata, new JacksonHandle(content)));
            }
            documents.add(docs);
        }
        return documents;
    }

    /**
     * @param connectionType the type of connection used to test
     * @param duration
     */
    private static void addDuration(String connectionType, long duration) {
        List<Long> durations = durationsMap.get(connectionType);
        if (durations == null) {
            durations = new ArrayList<>();
            durationsMap.put(connectionType, durations);
        }
        durations.add(duration);
        logger.info(connectionType + " duration: " + duration);
    }

    /**
     * Log all the durations and the average duration for each connection type.
     */
    private static void printDurations() {
        int documentCount = batchCount * batchSize;

        System.out.println("All durations for document count: " + documentCount);
        durationsMap.forEach((connectionType, durations) -> {
            System.out.println(connectionType + ": " + durations);
        });

        System.out.println("Average durations for document count: " + documentCount);
        durationsMap.forEach((connectionType, durations) -> {
            long total = durations.stream().reduce((value, accumulator) -> value + accumulator.longValue()).get();
            System.out.println(connectionType + ": " + (total / iterations));
        });
    }
}
