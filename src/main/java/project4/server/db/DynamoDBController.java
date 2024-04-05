package project4.server.db;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.Iterator;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DynamoDBController {
    // Create the session credentials object
    private AwsSessionCredentials awsSessionCredentials = AwsSessionCredentials.create(
            "", // Your access key ID
            "", // Your secret access key
            "" // Your session token
    );

    private DynamoDbClient dynamoDbClient = DynamoDbClient.builder()
            .credentialsProvider(StaticCredentialsProvider.create(awsSessionCredentials))
            .region(Region.of("us-west-2")) // Specify your region
            .build();

    public Integer querySkierVerticals(String skierId) {
        QueryRequest queryRequest = QueryRequest.builder()
                .tableName("skInfo")
                .keyConditionExpression("primKey = :skierId")
                .expressionAttributeValues(Map.of(":skierId", AttributeValue.builder().s(skierId).build()))
                .build();

        QueryResponse queryResponse = dynamoDbClient.query(queryRequest);
        List<Map<String, AttributeValue>> items = queryResponse.items();

        if (items.isEmpty()) {
            return null;
        }

        int vertical = 0;
        for (Map<String, AttributeValue> item : items) {
            vertical += Integer.parseInt(item.get("verticals").n());
        }

        return vertical;
    }

    public void updateSkIdTable(ConcurrentMap<String, List<String>> liftRidesMap) {
        int itemCount = 0;
        final int BATCH_SIZE = 25;
        List<WriteRequest> batch = new ArrayList<>(BATCH_SIZE);
        while (itemCount < 200000) {
            for (Iterator<Map.Entry<String, List<String>>> it = liftRidesMap.entrySet().iterator(); it.hasNext();) {
                Map.Entry<String, List<String>> entry = it.next();
                String skierId = entry.getKey();
                List<String> rides = entry.getValue();

                if (rides.size() < 5) {
                    log.error("Invalid ride data for skierId: " + skierId);
                    it.remove();
                    continue;
                }

                String resortID = rides.get(0);
                String seasonID = rides.get(1);
                String dayID = rides.get(2);
                String liftID = rides.get(3);
                String time = rides.get(4);
                String sortKey = String.join("#", resortID, seasonID, dayID, time);

                Map<String, AttributeValue> itemAttributes = Map.of(
                        "primKey", AttributeValue.builder().s(skierId).build(),
                        "sortKey", AttributeValue.builder().s(sortKey).build(),
                        "liftID", AttributeValue.builder().s(liftID).build(),
                        "verticals", AttributeValue.builder().n(String.valueOf(10 * Integer.parseInt(liftID))).build());

                PutRequest putRequest = PutRequest.builder().item(itemAttributes).build();
                WriteRequest writeRequest = WriteRequest.builder().putRequest(putRequest).build();

                synchronized (batch) {
                    batch.add(writeRequest);
                    if (batch.size() == BATCH_SIZE) {
                        submitBatch(batch);
                        batch.clear(); // Prepare for next batch
                    }
                }

                itemCount++;
                it.remove();
                log.info("item count: " + itemCount);
            }
        }
        if (!batch.isEmpty()) {
            submitBatch(batch);
        }
        log.info("Finished updating skInfo. Total items processed: " + itemCount);
    }

    private void submitBatch(List<WriteRequest> batch) {
        BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder()
                .requestItems(Map.of("skInfo", batch))
                .build();

        dynamoDbClient.batchWriteItem(batchWriteItemRequest);
    }
}
