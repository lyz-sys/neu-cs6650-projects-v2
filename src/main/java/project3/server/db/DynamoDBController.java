package project3.server.db;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import java.util.concurrent.ConcurrentMap;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

@Slf4j
public class DynamoDBController {
    private DynamoDbClient dynamoDbClient = DynamoDbClient.builder()
            .region(Region.of("us-west-2")) // Specify your region
            .build();

    public void updateSkIdTable(ConcurrentMap<String, List<String>> liftRidesMap) {
        int itemCount = 0; // todo: magic number here, may need to change
        while (itemCount <= 200000) {
            for (Map.Entry<String, List<String>> entry : liftRidesMap.entrySet()) {
                String skierId = entry.getKey();
                List<String> rides = entry.getValue();

                String resortID = rides.get(0);
                String seasonID = rides.get(1);
                String dayID = rides.get(2);
                String liftID = rides.get(3);
                String time = rides.get(4);

                PutItemRequest putItemRequest = PutItemRequest.builder()
                    .tableName("skier-table")
                    .item(Map.of(
                        "PK", AttributeValue.builder().s(skierId).build(), // skierId
                        "SK", AttributeValue.builder().s(String.join("#", resortID, seasonID, dayID, liftID, time)).build(), // Composite key
                        "verticals", AttributeValue.builder().n(String.valueOf(10 * Integer.parseInt(liftID))).build() // Assuming liftId is a numeric value
                        // Add other attributes as needed
                    ))
                    .build();

                dynamoDbClient.putItem(putItemRequest);

                liftRidesMap.remove(skierId);
                itemCount++;
                log.info("item count: " + itemCount);
            }
        }
    }
}
