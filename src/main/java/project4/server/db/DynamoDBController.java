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

@Slf4j
public class DynamoDBController {
    // Create the session credentials object
    private AwsSessionCredentials awsSessionCredentials = AwsSessionCredentials.create(
            "ASIAX3JQJHMPUF6IRJ5Z", // Your access key ID
            "upalMchiRmYU6qJdNUILa10q75DfXRUSWdFNgKbH", // Your secret access key
            "IQoJb3JpZ2luX2VjEBMaCXVzLXdlc3QtMiJHMEUCIBdVgyqFnUJ0BIGmb3xlGAYK72zbBNPEzBKjKpak/AEMAiEAyfC271FIFo7QC2xw1tCc1a6AwrhnVdyEazV/+SsCkL4qtAIITBAAGgw1Mzk2NTY1MzQ4MTUiDPanTS8lXhbGbE3k3SqRAhHxlJIN6py/UId3qd5mjZsOj02oLUbDv0gWJZKYxFgWjI/qjd80ccXeNZV/gJdq8P5bJJyD3okVG0HOLxbC8yriso3EkX9ZWpMgZi9zpN4YIhi6U4PMuCGyzFaTrqxl15XEOi+k41DgpdzziEt2ensUoD5l0LNZ5eYLXQy/g6ALzMwTi5cIq9ekBOlMd9pchtsH5C5NUnWW3bFeJZgoOxzoq01ARm9Pwjtf8sBLzJK2k9SN1VuTB3JFlWs7hWagq4lOZT0eEnOTEmYTrUl/f+9bDebSuj7bTsLJZGX7zmlooVrCEvkl9nwLdygfXW9abwOVvfgiyj0jjtpJ2d5+ojw7Tm48X1ZDqcTJtyLOCXKgDTDO5eCwBjqdAQArtl4jFx/cykjn0HLfneiLG9LGZ+rmlqMl/E1cQ2HhuY8KkDeZXAu5uRJsPRykUyUb+i8UKaMxb9QCsItxXe6k6J5LG8vezHRAflLYVxbZDp7RSFjAyVdyOpn3yRhOlmfaWtcKOypdYp7As0uoZW+ny8cn4+RpBb0CaVVQDf6dq9AeusbWDR/dgLrHV5Nl/6KB+8C2fM8h8xGs6jQ=" // Your session token
    );

    private DynamoDbClient dynamoDbClient = DynamoDbClient.builder()
            .credentialsProvider(StaticCredentialsProvider.create(awsSessionCredentials))
            .region(Region.of("us-west-2")) // Specify your region
            .build();

    public Integer querySkierVerticals(String skierId, VerticalBody verticalBody) {
        // Build the key condition expression and attribute values
        String keyConditionExpression = "primKey = :skierId";
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":skierId", AttributeValue.builder().s(skierId).build());

        // Prepare the filter expression based on resort and season if provided
        String filterExpression = "";
        expressionAttributeValues.put(":resort", AttributeValue.builder().s(verticalBody.getResort()).build());
        filterExpression = "resort = :resort";

        if (verticalBody.getSeason() != null && !verticalBody.getSeason().isEmpty()) {
            expressionAttributeValues.put(":season", AttributeValue.builder().s(verticalBody.getSeason()).build());
            filterExpression += " AND ";
            filterExpression += "season = :season";
        }

        // Build the query request
        QueryRequest.Builder queryBuilder = QueryRequest.builder()
                .tableName("skInfo")
                .keyConditionExpression(keyConditionExpression)
                .expressionAttributeValues(expressionAttributeValues);

        // Add filter expression if needed
        if (!filterExpression.isEmpty()) {
            queryBuilder.filterExpression(filterExpression);
        }

        QueryRequest queryRequest = queryBuilder.build();
        QueryResponse queryResponse = dynamoDbClient.query(queryRequest);
        List<Map<String, AttributeValue>> items = queryResponse.items();

        if (items.isEmpty()) {
            return null;
        }

        // Calculate total verticals
        int vertical = 0;
        for (Map<String, AttributeValue> item : items) {
            vertical += Integer.parseInt(item.get("verticals").s());
        }

        return vertical;
    }

    public Integer queryASkierVertical(String skierID, String resortID, String seasonID, String dayID) {
        // Assuming 'primKey' is a combination of skierID, resortID, seasonID, and dayID
        // This example will need adjustment if your table uses different key structures
        String keyConditionExpression = "skierId = :skierId AND resortId = :resortId AND seasonId = :seasonId AND dayId = :dayId";
        
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":skierId", AttributeValue.builder().s(skierID).build());
        expressionAttributeValues.put(":resortId", AttributeValue.builder().s(resortID).build());
        expressionAttributeValues.put(":seasonId", AttributeValue.builder().s(seasonID).build());
        expressionAttributeValues.put(":dayId", AttributeValue.builder().s(dayID).build());
    
        QueryRequest queryRequest = QueryRequest.builder()
                .tableName("skInfo")
                .keyConditionExpression(keyConditionExpression)
                .expressionAttributeValues(expressionAttributeValues)
                .build();
    
        QueryResponse queryResponse = dynamoDbClient.query(queryRequest);
        List<Map<String, AttributeValue>> items = queryResponse.items();
    
        if (items.isEmpty()) {
            return null;
        }
    
        // Assuming there is only one item because the query is for a specific record
        Map<String, AttributeValue> item = items.get(0);
        int vertical = Integer.parseInt(item.get("verticals").n()); // Using .n() assuming 'verticals' is stored as Number in DynamoDB
    
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
