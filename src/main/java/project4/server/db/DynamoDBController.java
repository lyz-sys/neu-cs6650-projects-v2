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
import project4.server.servlets.SkierServlet.VerticalBody;
import java.util.logging.Logger;
import java.util.logging.Level;

@Slf4j
public class DynamoDBController {
    private static final Logger logger = Logger.getLogger(DynamoDBController.class.getName());

    // Create the session credentials object
    private AwsSessionCredentials awsSessionCredentials = AwsSessionCredentials.create(
            "ASIAX3JQJHMP3W3EPJSY", // Your access key ID
            "+7ryjvrr8TmFnlJxt+zfILySDSTbdxFIeKnFfXlF", // Your secret access key
            "IQoJb3JpZ2luX2VjECsaCXVzLXdlc3QtMiJHMEUCIFtCV5Byi2dQTXmGgyt1w5SRQ/vXQZ1Tlt+sEJfvuHqxAiEA2Xx+ek3m5mcXArUlgEVugSdc+tVQ1PpVGOyMPrZRC4MqtAIIZBAAGgw1Mzk2NTY1MzQ4MTUiDAz8LfCP1cAkpMt8hSqRAoo/bBMGFJWrizN+T8yApLQ+hzweSa5VrYB+w4uCl6ujEVeR2lWKnvtuGbQA8G7TVnxIXuVzBjqDcZ0uMTXr4SQ4+FRPe2bPVzqauS/rHFakNRaZ7mFA7q8hS/wJhr3x/mqE+KZTD8czlHBSlBX4IozM/GlaT2/8oVhl7Vi38AfAeySdPa9dHt3A+nOvmW+8A0tFO+NvEEgsOWQI1yOiWLlN+45z4dAg64pHo8WKYvQpCc+L7qFIfgBOJYqsua1JZUA7cz+Q9GKw2l4Wvqq44rZyB9SNdTIZZaWO1RSfdnD1el0BTjlnKjW+HQXERqxeRJqjTjQVso6nuwE9w2Xbzul9XjX1J9PIkezygrsYw3H8XDC//uWwBjqdATi3YSXnxL0F2NPpnX055kwBGwMJHsD8mmcG4JJ/OacmZhmq9uYFZeF2FIr721tNPknp4hUbjtBeTDT2Y2i8ctzQqA0NOskrY3U70IA1uVo1HE9huPpKlgDWdhJLuQXb9YCgriCX2UlU3Xzu8EEcFGrEuQC3BDv5Q6zAf/JxsE69WZg1fFjEBsL7oJ9Q9DbohaZoQAdzFDnUVkb9E6Y=" // Your session token
    );

    private DynamoDbClient dynamoDbClient = DynamoDbClient.builder()
            .credentialsProvider(StaticCredentialsProvider.create(awsSessionCredentials))
            .region(Region.of("us-west-2")) // Specify your region
            .build();

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

    public Integer getTotalVerticalForSkier(String skierId, String resortId, List<String> seasonIds) {
        int totalVertical = 0;

        for (String seasonId : seasonIds) {
            String gsiPartitionKey = skierId;
            String gsiSortKeyPrefix = resortId + "_" + seasonId;

            // Log the variables before the query
            logger.log(Level.INFO, "Querying GSI with skierId: {0}, resortId: {1}, seasonId: {2}",
                    new Object[] { skierId, resortId, seasonId });

            QueryRequest queryRequest = QueryRequest.builder()
                    .tableName("SkiActivities")
                    .indexName("skierId-resortId_seasonId-index")
                    .keyConditionExpression(
                            "skierId = :gsiPartitionKey AND begins_with(resortId_seasonId_dayId, :gsiSortKeyPrefix)")
                    .expressionAttributeValues(Map.of(
                            ":gsiPartitionKey", AttributeValue.builder().s(gsiPartitionKey).build(),
                            ":gsiSortKeyPrefix", AttributeValue.builder().s(gsiSortKeyPrefix).build()))
                    .build();

            QueryResponse queryResponse = dynamoDbClient.query(queryRequest);

            if (queryResponse.hasItems()) {
                int seasonVertical = queryResponse.items().stream()
                        .mapToInt(item -> Integer.parseInt(item.get("verticals").n()))
                        .sum();
                // Log the result for each season
                logger.log(Level.INFO, "Total vertical for season {0}: {1}", new Object[] { seasonId, seasonVertical });
                totalVertical += seasonVertical;
            } else {
                // Log when no items are found
                logger.log(Level.WARNING, "No items found for skierId: {0}, resortId: {1}, seasonId: {2}",
                        new Object[] { skierId, resortId, seasonId });
            }
        }

        // Log the final total vertical
        logger.log(Level.INFO, "Total vertical for skier {0}: {1}", new Object[] { skierId, totalVertical });
        return totalVertical;
    }

    public Integer getSkiDayVerticalForSkier(String resortId, String seasonId, String dayId, String skierId) {
        String partitionKey = resortId + "_" + seasonId + "_" + dayId;

        // Querying the primary key
        QueryRequest queryRequest = QueryRequest.builder()
                .tableName("SkiActivities")
                .keyConditionExpression("resortId_seasonId_dayId = :partitionKey AND skierId = :skierId")
                .expressionAttributeValues(Map.of(
                        ":partitionKey", AttributeValue.builder().s(partitionKey).build(),
                        ":skierId", AttributeValue.builder().s(skierId).build()))
                .build();

        QueryResponse queryResponse = dynamoDbClient.query(queryRequest);
        return queryResponse.items().stream()
                .findFirst()
                .map(item -> Integer.parseInt(item.get("verticals").n()))
                .orElse(0); // Returns 0 if there is no record found
    }

    public Integer getUniqueSkierCount(String resortId, String seasonId, String dayId) {
        String partitionKey = resortId + "_" + seasonId + "_" + dayId;
        String uniqueSkiersKey = "UNIQUE_COUNT"; // This is a constant placeholder for unique skier count items

        // Querying the primary key
        QueryRequest queryRequest = QueryRequest.builder()
                .tableName("SkiActivities")
                .keyConditionExpression("resortId_seasonId_dayId = :partitionKey AND skierId = :uniqueSkiersKey")
                .expressionAttributeValues(Map.of(
                        ":partitionKey", AttributeValue.builder().s(partitionKey).build(),
                        ":uniqueSkiersKey", AttributeValue.builder().s(uniqueSkiersKey).build()))
                .build();

        QueryResponse queryResponse = dynamoDbClient.query(queryRequest);
        return queryResponse.items().stream()
                .findFirst()
                .map(item -> Integer.parseInt(item.get("uniqueSkiers").n()))
                .orElse(0); // Returns 0 if there is no record found
    }
}
