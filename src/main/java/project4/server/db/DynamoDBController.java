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
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;

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
                        "ASIAX3JQJHMPRF6POUM2", // Your access key ID
                        "LDZ/rCovTtwcnSgXfcgDjNdaJFmmINhHk9zXTezI", // Your secret access key
                        "IQoJb3JpZ2luX2VjEHIaCXVzLXdlc3QtMiJIMEYCIQCzd2iqJMNv34P3usRSKUGK1+o9x/GgKwRSsTvNV8fF7AIhALKH5JMjwfo2urWlU6c0C70oSOnMwiRJxC72IM3q+da9Kr0CCKv//////////wEQABoMNTM5NjU2NTM0ODE1IgzuhS+NTga62Zqy7ZgqkQLulh0hPnna9Y7VHmILTjTfqEua1jk6TyFUGe4BNdpiZqRxVyM7wAaQPaQckyN17VwjLuhYqIunm5htUUqb1yJQTvddW9DRJRJrHv8uWx7WS+B6w6ZcV+0sMCjOZoLHs/a6eIyV65bx+x+a1thqIlake7VrFgchlNMXtZ2mztPYGYtKg5oLd9r2cxdQlNfuG3lxVt8mYDpTU4M2gmjw/NyfVllTr7Y4/gvhPVRgsFQOy2NKJH1+EP1IlBC/Lf0HGSdJ04+AQNiKAZWQk+LDpuVL7Axp0lbHHFfLwr4kDmWvZCn7b0ryLwBvsarvjLiRwePMd6voncjrM7GOZ4v/fAbFQ//4uc76X9d+dI2mlW0qPsMw+8/1sAY6nAHXFWeDWhpbyY/o49775VDLDDdMXNrJ0SpU5pM3m/nnhweLyjbClTY4v2M/iv6Uy8ngm/HEZEtrf4ny6y7WfL5VQxLbmTa+TFod5fkFw0DhRhQ3oGYd2iwGVnSQ1tddm0/isArE9fodFo4kRxICbTAQ2wKWMbE0vUMB+TyVLa3OYbIQ6vd8EYHa2EbAIpB8j0n9hZzM6AIt5XOf0dY=" // Your
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           // session
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           // token
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
                        for (Iterator<Map.Entry<String, List<String>>> it = liftRidesMap.entrySet().iterator(); it
                                        .hasNext();) {
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

                                String resortId_seasonId_dayId = String.join("_", resortID, seasonID, dayID);
                                Integer uniqueSkiers = -1;
                                Integer verticals = 10 * Integer.parseInt(liftID);

                                Map<String, AttributeValue> itemAttributes = Map.of(
                                                "resortId_seasonId_dayId",
                                                AttributeValue.builder().s(resortId_seasonId_dayId).build(),
                                                "skierId", AttributeValue.builder().s(skierId).build(),
                                                "uniqueSkiers",
                                                AttributeValue.builder().n(String.valueOf(uniqueSkiers)).build(),
                                                "verticals",
                                                AttributeValue.builder().n(String.valueOf(verticals)).build());

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

                                // Update the uniqueSkiers count
                                updateUniqueSkierCount(resortId_seasonId_dayId);

                                log.info("item count: " + itemCount);
                        }
                }
                if (!batch.isEmpty()) {
                        submitBatch(batch);
                }
                log.info("Finished updating skInfo. Total items processed: " + itemCount);
        }

        private void updateUniqueSkierCount(String resortId_seasonId_dayId) {
                // The key to look for.
                Map<String, AttributeValue> key = new HashMap<>();
                key.put("resortId_seasonId_dayId", AttributeValue.builder().s(resortId_seasonId_dayId).build());
                key.put("skierId", AttributeValue.builder().s("UNIQUE_COUNT").build());

                try {
                        // Try to increment the counter if the item exists.
                        UpdateItemRequest updateItemRequest = UpdateItemRequest.builder()
                                        .tableName("SkiActivities")
                                        .key(key)
                                        .updateExpression(
                                                        "SET uniqueSkiers = if_not_exists(uniqueSkiers, :start) + :inc")
                                        .expressionAttributeValues(Map.of(
                                                        ":start", AttributeValue.builder().n("0").build(),
                                                        ":inc", AttributeValue.builder().n("1").build()))
                                        .build();

                        dynamoDbClient.updateItem(updateItemRequest);
                } catch (DynamoDbException e) {
                        // If the unique count item doesn't exist, let's create it.
                        if (e.awsErrorDetails().errorCode().equals("ValidationException")) {
                                try {
                                        // Create the unique count item with a starting count of 1.
                                        PutItemRequest putItemRequest = PutItemRequest.builder()
                                                        .tableName("SkiActivities")
                                                        .item(key) // Reuse the 'key' as it has both keys needed for the
                                                                   // item
                                                        .conditionExpression(
                                                                        "attribute_not_exists(resortId_seasonId_dayId) AND attribute_not_exists(skierId)")
                                                        .build();

                                        dynamoDbClient.putItem(putItemRequest);
                                } catch (DynamoDbException putException) {
                                        // Handle exception for PutItem (e.g., item might have been created in the
                                        // meantime)
                                        log.error("Failed to create unique skiers count for: "
                                                        + resortId_seasonId_dayId, putException);
                                }
                        } else {
                                // Handle other exceptions.
                                log.error("Failed to ensure unique skiers count for: " + resortId_seasonId_dayId, e);
                        }
                }
        }

        private void submitBatch(List<WriteRequest> batch) {
                BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder()
                                .requestItems(Map.of("SkiActivities", batch))
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
                                                        ":gsiPartitionKey",
                                                        AttributeValue.builder().s(gsiPartitionKey).build(),
                                                        ":gsiSortKeyPrefix",
                                                        AttributeValue.builder().s(gsiSortKeyPrefix).build()))
                                        .build();

                        QueryResponse queryResponse = dynamoDbClient.query(queryRequest);

                        if (queryResponse.hasItems()) {
                                int seasonVertical = queryResponse.items().stream()
                                                .mapToInt(item -> Integer.parseInt(item.get("verticals").n()))
                                                .sum();
                                // Log the result for each season
                                logger.log(Level.INFO, "Total vertical for season {0}: {1}",
                                                new Object[] { seasonId, seasonVertical });
                                totalVertical += seasonVertical;
                        } else {
                                // Log when no items are found
                                logger.log(Level.WARNING,
                                                "No items found for skierId: {0}, resortId: {1}, seasonId: {2}",
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
                                .keyConditionExpression(
                                                "resortId_seasonId_dayId = :partitionKey AND skierId = :skierId")
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
                                .keyConditionExpression(
                                                "resortId_seasonId_dayId = :partitionKey AND skierId = :uniqueSkiersKey")
                                .expressionAttributeValues(Map.of(
                                                ":partitionKey", AttributeValue.builder().s(partitionKey).build(),
                                                ":uniqueSkiersKey",
                                                AttributeValue.builder().s(uniqueSkiersKey).build()))
                                .build();

                QueryResponse queryResponse = dynamoDbClient.query(queryRequest);
                return queryResponse.items().stream()
                                .findFirst()
                                .map(item -> Integer.parseInt(item.get("uniqueSkiers").n()))
                                .orElse(0); // Returns 0 if there is no record found
        }
}
