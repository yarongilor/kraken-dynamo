/**
 * Copyright 2010-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * This file is licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License. A copy of
 * the License is located at
 *
 * http://aws.amazon.com/apache2.0/
 *
 * This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
*/

package com.scylladb.alternator;

import static com.scylladb.alternator.StreamsAdapterDemoHelper.createTable;
import static com.scylladb.alternator.StreamsAdapterDemoHelper.deleteItem;
import static com.scylladb.alternator.StreamsAdapterDemoHelper.describeTable;
import static com.scylladb.alternator.StreamsAdapterDemoHelper.putItem;
import static com.scylladb.alternator.StreamsAdapterDemoHelper.putItems;
import static com.scylladb.alternator.StreamsAdapterDemoHelper.scanTable;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;
import static software.amazon.awssdk.services.dynamodb.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER;
import static software.amazon.awssdk.services.dynamodb.model.ShardIteratorType.TRIM_HORIZON;

import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientAsyncConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.StreamDescription;
import software.amazon.awssdk.services.dynamodb.model.StreamRecord;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsAsyncClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsAsyncClientBuilder;

public class StreamsAdapterDemo2 {
    private final static Logger LOGGER = LoggerFactory.getLogger(StreamsAdapterDemo2.class.getName());

    public static <U> CompletableFuture<U> failedFuture(Throwable ex) {
        CompletableFuture<U> f = new CompletableFuture<U>();
        f.completeExceptionally(ex);
        return f;
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        ArgumentParser parser = ArgumentParsers.newFor("StreamAdapterDemo").build().defaultHelp(true)
                .description("Replicate a simple table using DynamoDB/Alternator Streams.");

        parser.addArgument("--aws").action(storeTrue()).help("Run against AWS");
        parser.addArgument("-c", "--cloudwatch").setDefault(false).help("Enable Cloudwatch");
        parser.addArgument("-e", "--endpoint").setDefault(new URL("http://localhost:8000"))
                .help("DynamoDB/Alternator endpoint");
        parser.addArgument("-se", "--streams-endpoint").help("DynamoDB/Alternator streams endpoint");

        parser.addArgument("-u", "--user").setDefault("none").help("Credentials username");
        parser.addArgument("-p", "--password").setDefault("none").help("Credentials password");
        parser.addArgument("-r", "--region").setDefault("us-east-1").help("AWS region");
        parser.addArgument("-t", "--table-prefix").setDefault("KCL-Demo").help("Demo table name prefix");

        parser.addArgument("-k", "--key-number").type(Integer.class).setDefault(0)
                .help("number of key in the src table");

        parser.addArgument("--timeout").type(Integer.class).setDefault(0).help("number of key in the src table");

        parser.addArgument("--create").action(storeTrue()).help("Create source data set if not available");
        parser.addArgument("--threads").type(Integer.class).setDefault(Runtime.getRuntime().availableProcessors() * 2)
                .help("Max worker threads");
        Namespace ns = null;
        try {
            ns = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }

        String tablePrefix = ns.getString("table_prefix");
        int keyNumber = ns.getInt("key_number");
        int timeoutInSeconds = ns.getInt("timeout");
        int threads = ns.getInt("threads");
        boolean create_data = ns.getBoolean("create");

        Region region = Region.of(ns.getString("region"));
        DynamoDbAsyncClientBuilder b = DynamoDbAsyncClient.builder().region(region);
        DynamoDbStreamsAsyncClientBuilder sb = DynamoDbStreamsAsyncClient.builder().region(region);

        ExecutorService executor = newFixedThreadPool(threads);
        ClientAsyncConfiguration cas = ClientAsyncConfiguration.builder()
                .advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, executor).build();
        b.asyncConfiguration(cas);
        sb.asyncConfiguration(cas);

        if (!ns.getBoolean("aws")) {
            if (ns.getString("endpoint") != null) {
                URI endpoint = URI.create(ns.getString("endpoint"));
                AlternatorAsyncHttpClient.Builder cb = AlternatorAsyncHttpClient.builder(endpoint);
                b.httpClientBuilder(cb);
                sb.httpClientBuilder(cb);
                b.endpointOverride(endpoint);
                sb.endpointOverride(endpoint);
            }
            if (ns.getString("streams_endpoint") != null) {
                URI endpoint = URI.create(ns.getString("streams_endpoint"));
                AlternatorAsyncHttpClient.Builder cb = AlternatorAsyncHttpClient.builder(endpoint);
                sb.httpClientBuilder(cb);
                sb.endpointOverride(endpoint);
            }
            if (ns.getString("user") != null) {
                AwsCredentialsProvider cp = StaticCredentialsProvider
                        .create(AwsBasicCredentials.create(ns.getString("user"), ns.getString("password")));
                b.credentialsProvider(cp);
                sb.credentialsProvider(cp);
            }
        }

        LOGGER.info("Starting demo...");

        String srcTable = tablePrefix;
        String destTable = tablePrefix + "-dest";

        DynamoDbStreamsAsyncClient adapterClient = sb.build();
        DynamoDbAsyncClient dynamoDBClient = b.build();

        try {
            // for sanity, and clarity, we'll do the main loop synchrounous.
            String streamArn = setUpTables(dynamoDBClient, tablePrefix).get();

            LOGGER.info("Creating worker for stream: " + streamArn);

            StreamScanner scanner = new StreamScanner(adapterClient, streamArn, (records) -> {
                return processRecords(dynamoDBClient, records, destTable);
            });

            LOGGER.info("Starting worker...");

            if (keyNumber != 0 || create_data) {
                ScanResponse sr = scanTable(dynamoDBClient, srcTable).get();
                ScanResponse dr = null;

                if (sr.count() < keyNumber && create_data) {
                    LOGGER.info("Adding {} records to source table...", keyNumber - sr.count());
                    putItems(dynamoDBClient, tablePrefix, sr.count(), keyNumber).get();
                }

                CompletableFuture<Long> scanResult = null;

                long start = System.currentTimeMillis();

                for (;;) {
                    if (scanResult != null && scanResult.isDone()) {
                        long nr = scanResult.get();
                        LOGGER.debug("Scanned {} records", nr);
                    }
                    if (scanResult == null || scanResult.isDone()) {
                        scanResult = scanner.process();
                    }
                    try {
                        scanResult.get(10, SECONDS);
                    } catch (TimeoutException e) {
                    }

                    dr = scanTable(dynamoDBClient, destTable).get();
                    LOGGER.info("keys synced: {}/{}", dr.count(), keyNumber);
                    if (!dr.count().equals(keyNumber)) {
                        continue;
                    }
                    if (dr.count() >= keyNumber) {
                        break;
                    }

                    if (timeoutInSeconds != 0) {
                        long now = System.currentTimeMillis();
                        if ((now - start) >= timeoutInSeconds * 1000) {
                            LOGGER.error("Timeout reached. Exiting...");
                            System.exit(1);
                        }
                    }
                }

                long now = System.currentTimeMillis();
                LOGGER.info("Sync took {} ms.", now - start);

                scanner.shutdown();

                if (create_data) {
                    if (sr.count() < keyNumber) {
                        sr = scanTable(dynamoDBClient, srcTable).get();
                    }
                    if (dr != null && sr.items().equals(dr.items())) {
                        LOGGER.info("Scan result is equal.");
                    } else {
                        LOGGER.error("Tables are different!");
                    }
                }

                LOGGER.info("Shutting down Worker");
                if (scanResult != null) {
                    scanResult.get();
                }
            }
            executor.shutdown();
            SCHEDULER.shutdown();
            LOGGER.info("Done.");
        } finally {
            // cleanup(dynamoDBClient, tablePrefix);
        }
    }

    private static CompletableFuture<Void> processRecords(DynamoDbAsyncClient dynamoDBClient, List<Record> records,
            String tableName) {
        CompletableFuture<Void> f = completedFuture(null);

        for (Record r : records) {
            StreamRecord sr = r.dynamodb();
            switch (r.eventName()) {
            case INSERT:
            case MODIFY:
                f = f.thenCompose(
                        (v) -> write(r, () -> putItem(dynamoDBClient, tableName, sr.newImage()), NUM_RETRIES));
                break;
            case REMOVE:
                f = f.thenCompose((v) -> write(r, () -> deleteItem(dynamoDBClient, tableName, sr.keys()), NUM_RETRIES));
                break;
            default:
                break;
            }
        }
        return f;
    }

    private static <T> CompletableFuture<Void> write(Record r, Supplier<CompletableFuture<T>> func, int retries) {
        return func.get().handle((res, t) -> {
            if (t != null) {
                LOGGER.warn("Caught throwable while processing record: " + r, t);
                return null;
            }
            return res;
        }).thenCompose((res) -> {
            if (res == null) {
                if (retries > 0) {
                    return CompletableFuture
                            .supplyAsync(() -> "apa", delayedExecutor(BACKOFF_TIME_IN_MILLIS, MILLISECONDS))
                            .thenCompose((ignore) -> {
                                return write(r, func, retries - 1);
                            });

                }
                LOGGER.error("Couldn't process record {}. Skipping.", r);
            }
            return completedFuture(null);
        });
    }

    private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
    private static final int NUM_RETRIES = 10;

    static class StreamScanner {
        private final DynamoDbStreamsAsyncClient client;
        private final String streamArn;
        private final Set<String> completedShards = new ConcurrentSkipListSet<>();
        private final Map<String, String> streamPositions = new ConcurrentHashMap<>();
        private final Function<List<Record>, CompletableFuture<Void>> consumer;

        private boolean stop = false;

        private static final int RETRIES = 4;

        public StreamScanner(DynamoDbStreamsAsyncClient client, String streamArn,
                Function<List<Record>, CompletableFuture<Void>> consumer) {
            this.client = client;
            this.streamArn = streamArn;
            this.consumer = consumer;
        }

        public void shutdown() {
            this.stop = true;
        }

        CompletableFuture<DescribeStreamResponse> describeStream(String start, List<Shard> shards) {
            return client
                    .describeStream(
                            DescribeStreamRequest.builder().streamArn(streamArn).exclusiveStartShardId(start).build())
                    .thenCompose((response) -> {
                        StreamDescription desc = response.streamDescription();
                        shards.addAll(desc.shards().stream().filter(s -> !completedShards.contains(s.shardId()))
                                .collect(toList()));
                        if (desc.lastEvaluatedShardId() != null) {
                            return describeStream(desc.lastEvaluatedShardId(), shards);
                        }
                        return completedFuture(response);
                    });
        }

        private CompletableFuture<Long> scanShard(Shard s, Map<String, CompletableFuture<Long>> parents) {
            if (parents.containsKey(s.shardId())) {
                return parents.get(s.shardId()).thenCompose((ignore) -> {
                    return scanShard(s, emptyMap());
                });
            }

            GetShardIteratorRequest.Builder b = GetShardIteratorRequest.builder().streamArn(streamArn)
                    .shardId(s.shardId());
            String pos = streamPositions.get(s.shardId());
            if (pos != null) {
                b.shardIteratorType(AFTER_SEQUENCE_NUMBER).sequenceNumber(pos);
            } else {
                b.shardIteratorType(TRIM_HORIZON);
            }

            return client.getShardIterator(b.build()).thenCompose((iter) -> {
                return scanShard(s, iter.shardIterator(), 0);
            });
        }

        private CompletionStage<Long> scanShard(Shard s, String iter, long progress) {
            return scanShard(s, iter, progress, RETRIES);
        }

        private CompletionStage<Long> scanShard(Shard s, String iter, long progress, int retries) {
            return client.getRecords(GetRecordsRequest.builder().shardIterator(iter).build()).thenCompose((result) -> {
                long next = progress;

                CompletableFuture<Void> f = completedFuture(null);

                if (result.hasRecords()) {
                    List<Record> records = result.records();
                    next += records.size();
                    if (!records.isEmpty()) {
                        String newLastSeq = records.get(records.size() - 1).dynamodb().sequenceNumber();
                        streamPositions.put(s.shardId(), newLastSeq);
                        f = consumer.apply(records);
                    }
                }

                long np = next;

                if (result.nextShardIterator() == null) {
                    completedShards.add(s.shardId());
                    streamPositions.remove(s.shardId());
                    return f.thenCompose((v) -> completedFuture(np));
                }

                if (!result.hasRecords() || stop) {
                    if (retries == 0 || stop) {
                        return f.thenCompose((v) -> completedFuture(np));
                    }
                    return CompletableFuture.supplyAsync(() -> "apa", delayedExecutor(500, MILLISECONDS))
                            .thenCompose((ignore) -> {
                                return scanShard(s, result.nextShardIterator(), np, retries - 1);
                            });
                }
                return f.thenCompose((v) -> scanShard(s, result.nextShardIterator(), np, retries));
            });
        }

        CompletableFuture<Long> process() {
            List<Shard> shards = new ArrayList<>();
            Map<String, CompletableFuture<Long>> parents = new HashMap<>();
            return describeStream(null, shards).thenCompose((ignore) -> {
                for (Shard s : shards) {
                    if (s.parentShardId() != null) {
                        parents.put(s.parentShardId(), new CompletableFuture<Long>());
                    }
                }

                CompletableFuture<Long> f = completedFuture(0L);
                for (Shard s : shards) {
                    f = f.thenCombine(scanShard(s, parents), (c1, c2) -> {
                        return c1 + c2;
                    });
                }

                return f;
            });
        }
    }

    // exists in class library in java 9+, but we're jdk8 so re-implement wheel
    static final ScheduledExecutorService SCHEDULER = newScheduledThreadPool(0);

    static Executor delayedExecutor(long delay, TimeUnit unit) {
        return delayedExecutor(delay, unit, ForkJoinPool.commonPool());
    }

    static Executor delayedExecutor(long delay, TimeUnit unit, Executor executor) {
        return r -> SCHEDULER.schedule(() -> executor.execute(r), delay, unit);
    }

    private static CompletableFuture<DescribeTableResponse> awaitTableCreation(DynamoDbAsyncClient dynamoDBClient,
            String tableName) {
        return awaitTableCreation(dynamoDBClient, tableName, 100);
    }

    private static CompletableFuture<DescribeTableResponse> awaitTableCreation(DynamoDbAsyncClient dynamoDBClient,
            String tableName, int retries) {
        if (retries == 0) {
            return failedFuture(new TimeoutException("Timeout after table creation."));
        }

        return describeTable(dynamoDBClient, tableName).thenCompose((result) -> {
            boolean created = result.table().tableStatus() == TableStatus.ACTIVE;
            if (created) {
                LOGGER.info("Table is active.");
                return completedFuture(result);
            }
            return CompletableFuture.supplyAsync(() -> "apa", delayedExecutor(1, SECONDS)).thenCompose((s) -> {
                return awaitTableCreation(dynamoDBClient, tableName, retries - 1);
            });
        });
    }

    private static CompletableFuture<String> setUpTables(DynamoDbAsyncClient dynamoDBClient, String tablePrefix)
            throws TimeoutException, InterruptedException, ExecutionException {
        String srcTable = tablePrefix;
        String destTable = tablePrefix + "-dest";

        return createTable(dynamoDBClient, srcTable, true).thenCompose((streamArn) -> {
            return createTable(dynamoDBClient, destTable, false).thenCompose((destArn) -> {
                return awaitTableCreation(dynamoDBClient, srcTable).thenCompose((desc1) -> {
                    return awaitTableCreation(dynamoDBClient, destTable).thenCompose((desc2) -> {
                        return completedFuture(streamArn);
                    });
                });
            });
        });
    }

}
