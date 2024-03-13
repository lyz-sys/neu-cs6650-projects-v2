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
import java.util.Iterator;

@Slf4j
public class DynamoDBController {
    private DynamoDbClient dynamoDbClient = DynamoDbClient.builder()
            .region(Region.of("us-west-2")) // Specify your region
            .build();

    public void updateSkIdTable(ConcurrentMap<String, List<String>> liftRidesMap) {
        int itemCount = 0;
        while (!liftRidesMap.isEmpty() && itemCount < 200000) { // todo: may need to confirm the total items in the table
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
                    PutItemRequest putItemRequest = PutItemRequest.builder()
                            .tableName("skier-table")
                            .item(Map.of(
                                    "skierId", AttributeValue.builder().s(skierId).build(),
                                    "resortID", AttributeValue.builder().s(resortID).build(),
                                    "seasonID", AttributeValue.builder().s(seasonID).build(),
                                    "dayID", AttributeValue.builder().s(dayID).build(),
                                    "liftID", AttributeValue.builder().s(liftID).build(),
                                    "time", AttributeValue.builder().s(time).build(),
                                    "verticals",
                                    AttributeValue.builder().n(String.valueOf(10 * Integer.parseInt(liftID))).build()))
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
        log.info("Finished updating skier-table. Total items processed: " + itemCount);
    }
}
