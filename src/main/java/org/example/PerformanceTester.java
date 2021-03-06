package org.example;

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
import com.marklogic.mgmt.ManageClient;
import com.marklogic.mgmt.ManageConfig;
import com.marklogic.mgmt.resource.databases.DatabaseManager;
import com.marklogic.mgmt.resource.hosts.HostManager;
import com.marklogic.xcc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Runs performance tests for XCC, DMSDK, and Bulk that all involve writing docs to ML.
 */
public class PerformanceTester {

    private final static Logger logger = LoggerFactory.getLogger(PerformanceTester.class);

    // Initial default values; override these args
    private static boolean testXcc = true;
    private static boolean testDmsdk = true;
    private static boolean testBulk = true;
    private static boolean simpleBulkService = true;

    private static int batchCount = 10;
    private static int batchSize = 100;
    private static int iterations = 1;
    private static int threadCount = 24;
    private static int documentElements = 10;

    private static String host = "localhost";
    private static int restPort = 8003;
    private static int xdbcPort = 8004;
    private static String username = "admin";
    private static String password = "admin";

    private static final String COLLECTION = "data";

    private static ManageClient manageClient;
    private static DatabaseClient databaseClient;
    private static List<DatabaseClient> allDatabaseClients = new ArrayList<>();
    private static List<ContentSource> contentSources = new ArrayList<>();

    private static ObjectMapper objectMapper = new ObjectMapper();

    private static Map<String, List<Long>> durationsMap = new LinkedHashMap();

    public static void main(String[] args) {
        applyArgs(args);

        logger.info("Max JVM memory: " + Runtime.getRuntime().maxMemory());
        logger.info(String.format("Connecting to '%s' as user '%s'", host, username));
        logger.info("Batch count: " + batchCount);
        logger.info("Batch size: " + batchSize);
        logger.info("Thread count: " + threadCount);
        logger.info("Iterations: " + iterations);

        // Used by the DMSDK approach, for deleting data, and by the Bulk approach when there's only one host to connect to
        databaseClient = DatabaseClientFactory.newClient(host, restPort, new DatabaseClientFactory.DigestAuthContext(username, password));

        manageClient = new ManageClient(new ManageConfig(host, 8002, username, password));

        List<String> hostNames = new HostManager(manageClient).getHostNames();
        if (hostNames.size() > 1) {
            logger.info("For Bulk and XCC tests: multiple hosts detected, will connect to: " + hostNames);
            hostNames.forEach(hostName -> {
                allDatabaseClients.add(DatabaseClientFactory.newClient(hostName, restPort, new DatabaseClientFactory.DigestAuthContext(username, password)));
                contentSources.add(ContentSourceFactory.newContentSource(hostName, xdbcPort, username, password.toCharArray()));
            });
        } else {
            allDatabaseClients.add(databaseClient);
            contentSources.add(ContentSourceFactory.newContentSource(host, xdbcPort, username, password.toCharArray()));
        }

        deleteCollection();

        for (int i = 1; i <= iterations; i++) {
            int configuredBatchCount = batchCount;
            if (i == 1 && iterations > 1) {
                // Use a small number of batches on the first iteration, since it will be ignored in the average since
                // it is regularly higher than the other iterations for all 3 approaches.
                logger.info("For first iteration, using a batchCount of 100 since it will be ignored when calculating " +
                        "average duration for each approach.");
                batchCount = 100;
            }
            try {
                if (testXcc) {
                    testXcc();
                }
                if (testDmsdk) {
                    testWriteBatcher();
                }
                if (testBulk) {
                    testBulkInputCaller();
                }
            } finally {
                batchCount = configuredBatchCount;
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
                    case "documentElements":
                        documentElements = Integer.parseInt(tokens[1]);
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
    private static void deleteCollection() {
//        DeleteCollectionsJob job = new DeleteCollectionsJob(COLLECTION);
//        job.setThreadCount(32);
//        job.run(databaseClient);
        logger.info("Clearing database");
        new DatabaseManager(manageClient).clearDatabase("java-tester-content", true);
        logger.info("Finished clearing database");
    }

    /**
     * Uses a Java ExecutorService to support multi-threading in a fashion similar to that of DMSDK and the Bulk API.
     */
    private static void testXcc() {
        deleteCollection();

        ExecutorService executorService = Executors.newFixedThreadPool(threadCount * contentSources.size());

        // The same data is inserted so we don't spend a bunch of time creating Java objects, which muddies the water
        // when testing performance
        final ContentCreateOptions options = new ContentCreateOptions();
        options.setFormatXml();
        options.setCollections(new String[]{COLLECTION});
        final String xmlDocument = buildXmlDocument();

        int contentSourceCounter = 0;

        logger.info("Starting XCC test");
        long start = System.currentTimeMillis();
        for (int i = 0; i < batchCount; i++) {
            Content[] contentArray = new Content[batchSize];
            for (int j = 0; j < batchSize; j++) {
                String uuid = UUID.randomUUID().toString();
                contentArray[j] = ContentFactory.newContent(uuid + ".xml", xmlDocument, options);
            }

            ContentSource contentSource = contentSources.get(contentSourceCounter);
            contentSourceCounter++;
            if (contentSourceCounter >= contentSources.size()) {
                contentSourceCounter = 0;
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
        deleteCollection();

        DataMovementManager dataMovementManager = databaseClient.newDataMovementManager();
        WriteBatcher writeBatcher = dataMovementManager.newWriteBatcher()
                .withThreadCount(threadCount * allDatabaseClients.size())
                .withBatchSize(batchSize);

        // The same data is inserted so we don't spend a bunch of time creating Java objects, which muddies the water
        // when testing performance
        final DocumentMetadataHandle metadata = new DocumentMetadataHandle()
                .withCollections(COLLECTION, COLLECTION + "2")
                .withPermission("rest-reader", DocumentMetadataHandle.Capability.READ, DocumentMetadataHandle.Capability.UPDATE);
        final ObjectNode jsonDoc = buildJsonDocument();

        logger.info("Starting DMSDK test");
        long start = System.currentTimeMillis();
        for (int i = 0; i < batchCount; i++) {
            for (int j = 0; j < batchSize; j++) {
                writeBatcher.add(new DocumentWriteOperationImpl(
                        DocumentWriteOperation.OperationType.DOCUMENT_WRITE,
                        UUID.randomUUID() + ".json", metadata,
                        new JacksonHandle(jsonDoc)));
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
        deleteCollection();

        List<InputCaller.BulkInputCaller> bulkInputCallers = buildBulkInputCallers();

        // The same data is inserted so we don't spend a bunch of time creating Java objects, which muddies the water
        // when testing performance
        final ObjectNode jsonDoc = buildJsonDocument();
        final ObjectNode metadata = objectMapper.createObjectNode();
        metadata.putArray("collections").add(COLLECTION);
        ArrayNode permissions = metadata.putArray("permissions");
        permissions.addObject().put("roleName", "rest-reader").put("capability", "read");
        permissions.addObject().put("roleName", "rest-reader").put("capability", "update");

        int clientCounter = 0;

        logger.info("Starting Bulk test");
        long start = System.currentTimeMillis();
        for (int i = 0; i < batchCount; i++) {
            for (int j = 0; j < batchSize; j++) {
                InputCaller.BulkInputCaller bulkInputCaller = bulkInputCallers.get(clientCounter);
                clientCounter++;
                if (clientCounter >= bulkInputCallers.size()) {
                    clientCounter = 0;
                }

                if (!simpleBulkService) {
                    bulkInputCaller.accept(metadata);
                }
                bulkInputCaller.accept(jsonDoc);
            }
        }

        bulkInputCallers.forEach(caller -> caller.awaitCompletion());

        addDuration("Bulk", System.currentTimeMillis() - start);
    }

    private static List<InputCaller.BulkInputCaller> buildBulkInputCallers() {
        final StringHandle apiHandle = simpleBulkService ? new StringHandle(SIMPLE_BULK_API) : new StringHandle(COMPLEX_BULK_API);
        final JacksonHandle endpointConstants = new JacksonHandle(objectMapper.createObjectNode().put("simpleBulkService", simpleBulkService));

        List<InputCaller.BulkInputCaller> callers = new ArrayList<>();

        allDatabaseClients.forEach(client -> {
            InputCaller inputCaller;
            try {
                inputCaller = InputCaller.on(client, apiHandle, new JacksonHandle());
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }

            IOEndpoint.CallContext[] callContexts = new IOEndpoint.CallContext[threadCount];
            for (int i = 0; i < threadCount; i++) {
                callContexts[i] = inputCaller.newCallContext().withEndpointConstants(endpointConstants);
            }
            callers.add(inputCaller.bulkCaller(callContexts, threadCount));
        });

        return callers;
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

        System.out.println("Average durations (ignoring the first one) for document count: " + documentCount);
        durationsMap.forEach((connectionType, durations) -> {
            // Ignore first duration, which is frequently much larger for all 3 approaches
            if (durations.size() > 1) {
                durations = durations.subList(1, durations.size());
            }
            long total = durations.stream().reduce((value, accumulator) -> value + accumulator.longValue()).get();
            int denominator = iterations > 1 ? iterations - 1 : iterations;
            System.out.println(connectionType + ": " + (total / denominator));
        });
    }

    private static String buildXmlDocument() {
        StringBuilder xml = new StringBuilder("<root>");
        for (int i = 1; i <= documentElements; i++) {
            String name = String.format("element-%d", i);
            xml.append(String.format("<%s>test-%d</%s>", name, i, name));
        }
        return xml.append("</root>").toString();
    }

    private static ObjectNode buildJsonDocument() {
        ObjectNode doc = objectMapper.createObjectNode();
        for (int i = 1; i <= documentElements; i++) {
            doc.put(String.format("property-%d", i), String.format("test-%d", i));
        }
        return doc;
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
