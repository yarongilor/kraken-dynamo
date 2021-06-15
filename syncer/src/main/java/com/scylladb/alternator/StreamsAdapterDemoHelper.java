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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.StreamSpecification;
import software.amazon.awssdk.services.dynamodb.model.StreamViewType;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

public class StreamsAdapterDemoHelper {

    /**
     * @return StreamArn
     */
    public static CompletableFuture<String> createTable(DynamoDbAsyncClient client, String tableName,
            boolean enableStream) {
        java.util.List<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
        attributeDefinitions.add(AttributeDefinition.builder().attributeName("p").attributeType("S").build());

        java.util.List<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
        keySchema.add(KeySchemaElement.builder().attributeName("p").keyType(KeyType.HASH).build()); // Partition
        // key

        ProvisionedThroughput provisionedThroughput = ProvisionedThroughput.builder().readCapacityUnits(2L)
                .writeCapacityUnits(2L).build();

        StreamSpecification.Builder streamSpecification = StreamSpecification.builder();
        if (enableStream) {
            streamSpecification.streamEnabled(true);
            streamSpecification.streamViewType(StreamViewType.NEW_IMAGE);
        } else {
            streamSpecification.streamEnabled(false);
        }
        CreateTableRequest createTableRequest = CreateTableRequest.builder().tableName(tableName)
                .attributeDefinitions(attributeDefinitions).keySchema(keySchema)
                .provisionedThroughput(provisionedThroughput).streamSpecification(streamSpecification.build()).build();

        System.out.println("Creating table " + tableName);
        return client.createTable(createTableRequest).handle((result, t) -> {
            if (t != null) {
                t = t.getCause();
                if (t instanceof ResourceInUseException) {
                    return null;
                }
                throw new CompletionException(t);
            }
            return result;
        }).thenCompose((result) -> {
            if (result == null) {
                System.out.println("Table already exists.");
                return describeTable(client, tableName)
                        .thenCompose((response) -> completedFuture(response.table().latestStreamArn()));
            }
            return completedFuture(result.tableDescription().latestStreamArn());
        });
    }

    public static CompletableFuture<DescribeTableResponse> describeTable(DynamoDbAsyncClient client, String tableName) {
        return client.describeTable(DescribeTableRequest.builder().tableName(tableName).build());
    }

    private static CompletableFuture<ScanResponse> scanTable(DynamoDbAsyncClient dynamoDBClient, String tableName,
            ScanResponse prev) {
        ScanRequest.Builder b = ScanRequest.builder().tableName(tableName);
        if (prev.lastEvaluatedKey() != null && !prev.lastEvaluatedKey().isEmpty()) {
            b.exclusiveStartKey(prev.lastEvaluatedKey());
        }
        CompletableFuture<ScanResponse> rf = dynamoDBClient.scan(b.build());
        return rf.thenCompose((r) -> {
            if (r.count() == 0) {
                return completedFuture(prev);
            }
            if (prev.hasItems()) {
                r = r.toBuilder().items(concat(prev.items().stream(), r.items().stream()).collect(toList()))
                        .count(r.count() + prev.count()).build();
            }
            if (r.lastEvaluatedKey() != null && !r.lastEvaluatedKey().isEmpty()) {
                return scanTable(dynamoDBClient, tableName, r);
            }
            return completedFuture(r);
        });
    }

    public static CompletableFuture<ScanResponse> scanTable(DynamoDbAsyncClient dynamoDBClient, String tableName) {
        return scanTable(dynamoDBClient, tableName, ScanResponse.builder().count(0).build());
    }

    public static CompletableFuture<PutItemResponse> putItem(DynamoDbAsyncClient dynamoDBClient, String tableName,
            String id, String val) {
        return dynamoDBClient.putItem(putItem(tableName, items(id, val)));
    }

    public static CompletableFuture<PutItemResponse> putItem(DynamoDbAsyncClient client, String tableName,
            int recordNo) {
        return client.putItem(putItem(tableName, items(recordNo)));
    }

    public static Map<String, AttributeValue> items(String id, String val) {
        java.util.Map<String, AttributeValue> items = new HashMap<String, AttributeValue>();
        items.put("p", AttributeValue.builder().s(id).build());
        items.put("attribute-1", AttributeValue.builder().s(val).build());
        return items;
    }

    public static Map<String, AttributeValue> items(int recordNo) {
        return items(String.valueOf(recordNo), "le gris " + recordNo);
    }

    public static CompletableFuture<BatchWriteItemResponse> putItems(DynamoDbAsyncClient client, String tableName,
            int from, int to) {
        List<WriteRequest> items = new ArrayList<>(100);

        for (int e = Math.min(to, from + 100); from < e; ++from) {
            putItem(items, items(from));
        }

        CompletableFuture<BatchWriteItemResponse> res = batchWrite(client, tableName, items);
        if (from < to) {
            int nf = from;
            res = res.thenCompose((response) -> {
                return putItems(client, tableName, nf, to);
            });
        }
        return res;
    }

    public static CompletableFuture<BatchWriteItemResponse> batchWrite(DynamoDbAsyncClient client, String tableName,
            List<WriteRequest> items) {
        BatchWriteItemRequest r = BatchWriteItemRequest.builder()
                .requestItems(Collections.singletonMap(tableName, items)).build();
        return client.batchWriteItem(r);
    }

    public static void putItem(List<WriteRequest> dst, Map<String, AttributeValue> items) {
        dst.add(WriteRequest.builder().putRequest(PutRequest.builder().item(items).build()).build());
    }

    public static void deleteItem(List<WriteRequest> dst, Map<String, AttributeValue> keys) {
        dst.add(WriteRequest.builder().deleteRequest(DeleteRequest.builder().key(keys).build()).build());
    }

    public static CompletableFuture<PutItemResponse> putItem(DynamoDbAsyncClient dynamoDBClient, String tableName,
            Map<String, AttributeValue> items) {
        return dynamoDBClient.putItem(putItem(tableName, items));
    }

    public static PutItemRequest putItem(String tableName, Map<String, AttributeValue> items) {
        return PutItemRequest.builder().tableName(tableName).item(items).build();
    }

    public static CompletableFuture<UpdateItemResponse> updateItem(DynamoDbAsyncClient dynamoDBClient, String tableName,
            String id, String val) {
        java.util.Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
        key.put("p", AttributeValue.builder().n(id).build());

        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<String, AttributeValueUpdate>();
        AttributeValueUpdate update = AttributeValueUpdate.builder().action(AttributeAction.PUT)
                .value(AttributeValue.builder().s(val).build()).build();
        attributeUpdates.put("attribute-2", update);

        UpdateItemRequest updateItemRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
                .attributeUpdates(attributeUpdates).build();
        return dynamoDBClient.updateItem(updateItemRequest);
    }

    public static CompletableFuture<DeleteItemResponse> deleteItem(DynamoDbAsyncClient dynamoDBClient, String tableName,
            Map<String, AttributeValue> atts) {
        return deleteItem(dynamoDBClient, tableName, atts.get("p"));
    }

    public static CompletableFuture<DeleteItemResponse> deleteItem(DynamoDbAsyncClient dynamoDBClient, String tableName,
            AttributeValue keyValue) {
        java.util.Map<String, AttributeValue> key = new HashMap<String, AttributeValue>();
        key.put("p", keyValue);

        DeleteItemRequest deleteItemRequest = DeleteItemRequest.builder().tableName(tableName).key(key).build();
        return dynamoDBClient.deleteItem(deleteItemRequest);
    }

    public static CompletableFuture<DeleteItemResponse> deleteItem(DynamoDbAsyncClient dynamoDBClient, String tableName,
            String id) {
        return deleteItem(dynamoDBClient, tableName, AttributeValue.builder().s(id).build());
    }

}
