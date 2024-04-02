package project4.server.db;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
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
            "ASIAX3JQJHMPZHCHDZ22", // Your access key ID
            "MaQAZUxACRiSgha6yr7mui/DSwVdfO8dZU8EUeUC", // Your secret access key
            "FwoGZXIvYXdzEL3//////////wEaDA9YU0tFUNh4oJdTUyLLATunUYGDkg6VnPchl8zl3hTlqIqtFHns8ZujwEYWYcVDSbZGO/ErFTths4dRtjzlbflfUr5GULAz0pTAqMe5hSeX3PWDFMOh6P0mk7O0u9pCrgWkWo93NuS2NcDNw2KzaLf8MXX4TehE3/QBnVFQi/9TcLXebWv9N3Nu372j/Sfw81CjRZPzkpZSI/x7VGhpBz0Bz6K0dLcJDfXPg1tSVcJHew0OO+fb6W5o3Ptgl/seOECqoGAnYo8FPMiRNzW2BdJq1i+NZYTjCdJlKKP3kbAGMi0ea83dfB/b2N57nM9AjDKpKOKALAbyCVxo0LQomZWQW3LS2eX/NP0tuVxUczA=" // Your
                                                                                                                                                                                                                                                                                                                                                                                                                      // session
                                                                                                                                                                                                                                                                                                                                                                                                                      // token
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

                try {
                    String sortKey = String.join("#", resortID, seasonID, dayID, time);

                    PutItemRequest putItemRequest = PutItemRequest.builder()
                            .tableName("skInfo")
                            .item(Map.of(
                                    "primKey", AttributeValue.builder().s(skierId).build(),
                                    "sortKey", AttributeValue.builder().s(sortKey).build(),
                                    "liftID", AttributeValue.builder().s(liftID).build(),
                                    "verticals", AttributeValue.builder().n(String.valueOf(10 * Integer.parseInt(liftID))).build()))
                            .build();

                    dynamoDbClient.putItem(putItemRequest);
                    itemCount++;
                } catch (Exception e) {
                    log.error("Error putting item to DynamoDB: " + e.getMessage(), e);
                } finally {
                    it.remove();
                }

                log.info("item count: " + itemCount);
            }
        }
        log.info("Finished updating skInfo. Total items processed: " + itemCount);
    }
}
