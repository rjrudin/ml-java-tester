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
import com.marklogic.client.io.StringHandle;
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
    private static boolean testXcc = true;
    private static boolean testDmsdk = true;
    private static boolean testBulk = true;
    private static boolean simpleBulkService = true;

    private static int batchCount = 100;
    private static int batchSize = 100;
    private static int iterations = 5;
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
            if (testXcc) {
                testXcc();
            }
            if (testDmsdk) {
                testWriteBatcher();
            }
            if (testBulk) {
                testBulkInputCaller();
            }
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
                    case "simpleBulkService":
                        simpleBulkService = Boolean.parseBoolean(tokens[1]);
                        break;
                    case "testXcc":
                        testXcc = Boolean.parseBoolean(tokens[1]);
                        break;
                    case "testDmsdk":
                        testDmsdk = Boolean.parseBoolean(tokens[1]);
                        break;
                    case "testBulk":
                        testBulk = Boolean.parseBoolean(tokens[1]);
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

        ContentCreateOptions options = new ContentCreateOptions();
        options.setFormatXml();
        options.setCollections(new String[]{COLLECTION});

        long start = System.currentTimeMillis();
        for (int i = 0; i < batchCount; i++) {
            Content[] contentArray = new Content[batchSize];
            for (int j = 0; j < batchSize; j++) {
                String uuid = UUID.randomUUID().toString();
                contentArray[j] = ContentFactory.newContent(uuid + ".xml", "<hello>world</hello>", options);
            }
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
        }

        waitForThreadsToFinish(executorService);
        addDuration("XCC", System.currentTimeMillis() - start);
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

        ObjectMapper objectMapper = new ObjectMapper();
        DocumentMetadataHandle metadata = new DocumentMetadataHandle()
                .withCollections(COLLECTION, COLLECTION + "2")
                .withPermission("rest-reader", DocumentMetadataHandle.Capability.READ, DocumentMetadataHandle.Capability.UPDATE);

        long start = System.currentTimeMillis();
        for (int i = 0; i < batchCount; i++) {
            for (int j = 0; j < batchSize; j++) {
                ObjectNode content = objectMapper.createObjectNode().put("hello", "world");
                writeBatcher.add(new DocumentWriteOperationImpl(DocumentWriteOperation.OperationType.DOCUMENT_WRITE,
                        UUID.randomUUID() + ".json", metadata, new JacksonHandle(content)));
            }
        }
        writeBatcher.flushAndWait();

        addDuration("DMSDK", System.currentTimeMillis() - start);
        dataMovementManager.stopJob(writeBatcher);
    }

    /**
     * Tests performance of writing data via a vanilla BulkInputCaller.
     */
    private static void testBulkInputCaller() {
        deleteData();

        ObjectMapper objectMapper = new ObjectMapper();
        InputCaller<JsonNode> inputCaller;

//        JacksonHandle apiHandle = databaseClient.newServerEval()
//                .javascript("xdmp.eval('cts.doc(\"/writeDocuments.api\")', {}, {database: xdmp.database('java-tester-modules')})")
//                .eval(new JacksonHandle());
//
        try {
            StringHandle apiHandle = simpleBulkService ? new StringHandle(SIMPLE_BULK_API) : new StringHandle(COMPLEX_BULK_API);
            inputCaller = InputCaller.on(databaseClient, apiHandle, new JacksonHandle());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        JacksonHandle endpointConstants = new JacksonHandle(
                objectMapper.createObjectNode().put("simpleBulkService", simpleBulkService)
        );

        IOEndpoint.CallContext[] callContexts = new IOEndpoint.CallContext[threadCount];
        for (int i = 0; i < threadCount; i++) {
            callContexts[i] = inputCaller.newCallContext().withEndpointConstants(endpointConstants);
        }
        InputCaller.BulkInputCaller<JsonNode> bulkInputCaller = inputCaller.bulkCaller(callContexts, threadCount);

        long start = System.currentTimeMillis();
        for (int i = 0; i < batchCount; i++) {
            for (int j = 0; j < batchSize; j++) {
                ObjectNode input = objectMapper.createObjectNode();
                if (simpleBulkService) {
                    input.put("hello", "world");
                    bulkInputCaller.accept(input);
                } else {
                    input.put("hello", "world");
                    ObjectNode metadata = objectMapper.createObjectNode();
                    metadata.put("uri", UUID.randomUUID() + ".json");
                    metadata.putArray("collections").add(COLLECTION);
                    ArrayNode permissions = metadata.putArray("permissions");
                    permissions.addObject().put("roleName", "rest-reader").put("capability", "read");
                    permissions.addObject().put("roleName", "rest-reader").put("capability", "update");
                    bulkInputCaller.accept(metadata);
                    bulkInputCaller.accept(input);
                }
            }
        }

        bulkInputCaller.awaitCompletion();
        addDuration("Bulk", System.currentTimeMillis() - start);
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

    private static final String SIMPLE_BULK_API = "{\n" +
            "  \"endpoint\": \"/writeDocuments.sjs\",\n" +
            "  \"params\": [\n" +
            "    {\n" +
            "      \"name\": \"endpointConstants\",\n" +
            "      \"datatype\": \"jsonDocument\",\n" +
            "      \"multiple\": false,\n" +
            "      \"nullable\": true\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"input\",\n" +
            "      \"datatype\": \"jsonDocument\",\n" +
            "      \"multiple\": true,\n" +
            "      \"nullable\": true\n" +
            "    }\n" +
            "  ],\n" +
            "  \"$bulk\": {\n" +
            "    \"inputBatchSize\": 100\n" +
            "  }\n" +
            "}";

    private static final String COMPLEX_BULK_API = "{\n" +
            "  \"endpoint\": \"/writeDocuments.sjs\",\n" +
            "  \"params\": [\n" +
            "    {\n" +
            "      \"name\": \"endpointConstants\",\n" +
            "      \"datatype\": \"jsonDocument\",\n" +
            "      \"multiple\": false,\n" +
            "      \"nullable\": true\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"input\",\n" +
            "      \"datatype\": \"jsonDocument\",\n" +
            "      \"multiple\": true,\n" +
            "      \"nullable\": true\n" +
            "    }\n" +
            "  ],\n" +
            "  \"$bulk\": {\n" +
            "    \"inputBatchSize\": 200\n" +
            "  }\n" +
            "}";
}
