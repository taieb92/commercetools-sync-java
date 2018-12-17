package com.commercetools.sync.benchmark;

import com.commercetools.sync.benchmark.helpers.CtpObserver;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Optional.ofNullable;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;

class BenchmarkUtils {
    private static final String BENCHMARK_RESULTS_FILE_NAME = "benchmarks.json";
    private static final String BENCHMARK_RESULTS_FILE_DIR = ofNullable(System.getenv("CI_BUILD_DIR"))
        .map(path -> path + "/tmp_git_dir/benchmarks/").orElse("");
    private static final String BENCHMARK_RESULTS_FILE_PATH = BENCHMARK_RESULTS_FILE_DIR + BENCHMARK_RESULTS_FILE_NAME;
    private static final Charset UTF8_CHARSET = StandardCharsets.UTF_8;

    private static final String SYNC_TOTAL_DURATION = "sync_total_duration";
    private static final String TOTAL_CTP_EXCHANGE_DURATION = "total_ctp_exchange_duration";
    private static final String AVG_CTP_EXCHANGE_DURATION = "avg_ctp_exchange_duration";
    private static final String NUMBER_OF_REQUESTS = "number_of_requests";
    private static final String NUMBER_OF_POSTS = "number_of_POSTs";
    private static final String NUMBER_OF_GETS = "number_of_GETs";
    private static final String NUMBER_OF_CREATES = "number_of_CREATES";
    private static final String NUMBER_OF_UPDATES = "number_of_UPDATES";
    private static final String NUMBER_OF_QUERIES = "number_of_QUERIES";

    static final String PRODUCT_SYNC = "productSync";
    static final String INVENTORY_SYNC = "inventorySync";
    static final String CATEGORY_SYNC = "categorySync";
    static final String TYPE_SYNC = "typeSync";
    static final String PRODUCT_TYPE_SYNC = "productTypeSync";

    static final String CREATES_ONLY = "createsOnly";
    static final String UPDATES_ONLY = "updatesOnly";
    static final String CREATES_AND_UPDATES = "mix";

    static final int THRESHOLD = 120000; //120 seconds in milliseconds //tODO: WILL CHANGE
    static final int NUMBER_OF_RESOURCE_UNDER_TEST = 1000;

    private static final String CURRENT_BRANCH = ofNullable(System.getenv("TRAVIS_BRANCH")).orElse("dev");


    static void saveNewResult(@Nonnull final String sync,
                              @Nonnull final String benchmark,
                              final double totalExecutionTime,
                              @Nonnull final CtpObserver ctpObserver) throws IOException {

        final JsonNode rootNode = new ObjectMapper().readTree(getFileContent(BENCHMARK_RESULTS_FILE_PATH));
        addNewResult(rootNode, sync, benchmark, totalExecutionTime, ctpObserver);
        writeToFile(rootNode.toString(), BENCHMARK_RESULTS_FILE_PATH);
    }

    private static void addNewResult(@Nonnull final JsonNode originalRoot,
                                         @Nonnull final String sync,
                                         @Nonnull final String benchmark,
                                         final double totalExecutionTime,
                                         @Nonnull final CtpObserver ctpObserver) {

        ObjectNode rootNodeChildren = (ObjectNode) originalRoot;
        ObjectNode branchNodeChildren = (ObjectNode) rootNodeChildren.get(CURRENT_BRANCH);

        // If version doesn't exist yet, create a new JSON object for the new version.
        if (branchNodeChildren == null) {
            branchNodeChildren = createBranchNodeChildren();
            rootNodeChildren.set(CURRENT_BRANCH, branchNodeChildren);
        }

        final ObjectNode syncNodeChildren = (ObjectNode) branchNodeChildren.get(sync);
        final ObjectNode benchmarkNodeChildren = (ObjectNode) syncNodeChildren.get(benchmark);

        benchmarkNodeChildren.set(SYNC_TOTAL_DURATION, JsonNodeFactory.instance.numberNode(totalExecutionTime));
        benchmarkNodeChildren.set(TOTAL_CTP_EXCHANGE_DURATION, JsonNodeFactory.instance.numberNode(ctpObserver.getTotalDuration().longValue()));
        benchmarkNodeChildren.set(AVG_CTP_EXCHANGE_DURATION, JsonNodeFactory.instance.numberNode(ctpObserver.getAverageDuration().longValue()));
        benchmarkNodeChildren.set(NUMBER_OF_REQUESTS, JsonNodeFactory.instance.numberNode(ctpObserver.getTotalNumberOfRequests().intValue()));
        benchmarkNodeChildren.set(NUMBER_OF_POSTS, JsonNodeFactory.instance.numberNode(ctpObserver.getTotalNumberOfPosts().intValue()));
        benchmarkNodeChildren.set(NUMBER_OF_GETS, JsonNodeFactory.instance.numberNode(ctpObserver.getTotalNumberOfGets().intValue()));
        benchmarkNodeChildren.set(NUMBER_OF_CREATES, JsonNodeFactory.instance.numberNode(ctpObserver.getTotalNumberOfCreateRequests().intValue()));
        benchmarkNodeChildren.set(NUMBER_OF_UPDATES, JsonNodeFactory.instance.numberNode(ctpObserver.getTotalNumberOfUpdateRequests().intValue()));
        benchmarkNodeChildren.set(NUMBER_OF_QUERIES, JsonNodeFactory.instance.numberNode(ctpObserver.getTotalNumberOfQueryRequests().intValue()));
        branchNodeChildren.set(AVG_CTP_EXCHANGE_DURATION, JsonNodeFactory.instance.numberNode(getBranchAvgCtpDuration(branchNodeChildren)));


    }

    private static long getBranchAvgCtpDuration(@Nonnull final JsonNode branchNode) {
        final long totalAvgCtpDuration = branchNode.get(PRODUCT_SYNC).get(UPDATES_ONLY).get(AVG_CTP_EXCHANGE_DURATION).asLong() +
            branchNode.get(PRODUCT_SYNC).get(CREATES_ONLY).get(AVG_CTP_EXCHANGE_DURATION).asLong() +
            branchNode.get(PRODUCT_SYNC).get(CREATES_AND_UPDATES).get(AVG_CTP_EXCHANGE_DURATION).asLong() +
            branchNode.get(CATEGORY_SYNC).get(UPDATES_ONLY).get(AVG_CTP_EXCHANGE_DURATION).asLong() +
            branchNode.get(CATEGORY_SYNC).get(CREATES_ONLY).get(AVG_CTP_EXCHANGE_DURATION).asLong() +
            branchNode.get(CATEGORY_SYNC).get(CREATES_AND_UPDATES).get(AVG_CTP_EXCHANGE_DURATION).asLong() +
            branchNode.get(INVENTORY_SYNC).get(UPDATES_ONLY).get(AVG_CTP_EXCHANGE_DURATION).asLong() +
            branchNode.get(INVENTORY_SYNC).get(CREATES_ONLY).get(AVG_CTP_EXCHANGE_DURATION).asLong() +
            branchNode.get(INVENTORY_SYNC).get(CREATES_AND_UPDATES).get(AVG_CTP_EXCHANGE_DURATION).asLong() +
            branchNode.get(TYPE_SYNC).get(UPDATES_ONLY).get(AVG_CTP_EXCHANGE_DURATION).asLong() +
            branchNode.get(TYPE_SYNC).get(CREATES_ONLY).get(AVG_CTP_EXCHANGE_DURATION).asLong() +
            branchNode.get(TYPE_SYNC).get(CREATES_AND_UPDATES).get(AVG_CTP_EXCHANGE_DURATION).asLong() +
            branchNode.get(PRODUCT_TYPE_SYNC).get(UPDATES_ONLY).get(AVG_CTP_EXCHANGE_DURATION).asLong() +
            branchNode.get(PRODUCT_TYPE_SYNC).get(CREATES_ONLY).get(AVG_CTP_EXCHANGE_DURATION).asLong() +
            branchNode.get(PRODUCT_TYPE_SYNC).get(CREATES_AND_UPDATES).get(AVG_CTP_EXCHANGE_DURATION).asLong();

        return totalAvgCtpDuration / 15;
    }

    @Nonnull
    private static <T> List<T> toList(@Nonnull final Iterator<T> iterator) {
        return toStream(iterator).collect(Collectors.toList());
    }

    @Nonnull
    private static <T> Stream<T> toStream(@Nonnull final Iterator<T> iterator) {
        return stream(spliteratorUnknownSize(iterator, Spliterator.ORDERED), false);
    }

    private static double calculateAvg(@Nonnull final List<JsonNode> results) {
        return results.stream().mapToDouble(JsonNode::asLong).average().orElse(0);
    }

    private static ObjectNode createBranchNodeChildren() {

        final HashMap<String, ObjectNode> nodes = new HashMap<>();
        nodes.put(PRODUCT_SYNC, createSyncNodeChildren());
        nodes.put(INVENTORY_SYNC, createSyncNodeChildren());
        nodes.put(CATEGORY_SYNC, createSyncNodeChildren());
        nodes.put(PRODUCT_TYPE_SYNC, createSyncNodeChildren());
        nodes.put(TYPE_SYNC, createSyncNodeChildren());

        return (ObjectNode) JsonNodeFactory.instance.objectNode().setAll(nodes);
    }

    private static ObjectNode createSyncNodeChildren() {

        final HashMap<String, ObjectNode> nodes = new HashMap<>();
        nodes.put(CREATES_ONLY, createBenchmarkNodeChildren());
        nodes.put(UPDATES_ONLY, createBenchmarkNodeChildren());
        nodes.put(CREATES_AND_UPDATES, createBenchmarkNodeChildren());

        return (ObjectNode) JsonNodeFactory.instance.objectNode().setAll(nodes);
    }

    private static ObjectNode createBenchmarkNodeChildren() {

        final ObjectNode newBenchmarkNode = JsonNodeFactory.instance.objectNode();
        newBenchmarkNode.set(SYNC_TOTAL_DURATION, JsonNodeFactory.instance.numberNode(0));
        newBenchmarkNode.set(TOTAL_CTP_EXCHANGE_DURATION, JsonNodeFactory.instance.numberNode(0));
        newBenchmarkNode.set(AVG_CTP_EXCHANGE_DURATION, JsonNodeFactory.instance.numberNode(0));
        newBenchmarkNode.set(NUMBER_OF_REQUESTS, JsonNodeFactory.instance.numberNode(0));
        newBenchmarkNode.set(NUMBER_OF_POSTS, JsonNodeFactory.instance.numberNode(0));
        newBenchmarkNode.set(NUMBER_OF_GETS, JsonNodeFactory.instance.numberNode(0));
        newBenchmarkNode.set(NUMBER_OF_CREATES, JsonNodeFactory.instance.numberNode(0));
        newBenchmarkNode.set(NUMBER_OF_UPDATES, JsonNodeFactory.instance.numberNode(0));
        return newBenchmarkNode;
    }

    private static String getFileContent(@Nonnull final String path) throws IOException {
        final byte[] fileBytes = Files.readAllBytes(Paths.get(path));
        return new String(fileBytes, UTF8_CHARSET);
    }

    private static void writeToFile(@Nonnull final String content, @Nonnull final String path) throws IOException {
        Files.write(Paths.get(path), content.getBytes(UTF8_CHARSET));
    }

    private BenchmarkUtils() {
    }
}
