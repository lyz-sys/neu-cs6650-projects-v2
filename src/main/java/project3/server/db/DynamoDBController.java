package project3.server.db;

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
            "ASIAX3JQJHMP4E7OVMF5", // Your access key ID
            "Ai6nkK1YSZPlJ5XwzXjOrrSO07YWHorblRb6RGYr", // Your secret access key
            "FwoGZXIvYXdzEIb//////////wEaDBB4Q9Gaj6w+C3+U+SLLAWkhWad0NnoQJX5C1SQI4x35TGtpCLj2Blq8aEwwyPhcGTeTEHtnbUTpw4S1LgiDsqGDivSEk+/mckPNiJ45tx/EQhTlQTnIQbz7FPYa73IZQv9uo6LSsCO4l9pwu3ywPS7DY7mWmpZrD7Q0ZEiEwD6fKa4AY1yduBcDUpxmBA/x2wiszdvIJ/qFbqvBH6pR1GrYbV3U1KJNrxKHfX2DwVbPpdibACoN1rBMzxLxYNCjwsWwFKylXxycT/l08v00uZW68odtP26Efz0nKKK2za8GMi3GU6T0XKKRCT5kgc2RnhHVEQYWjMHCOk3qDI4uvDWH2OgWv+rvbIP0qr6thoM" // Your
                                                                                                                                                                                                                                                                                                                                                                                                                      // session
                                                                                                                                                                                                                                                                                                                                                                                                                      // token
    );

    private DynamoDbClient dynamoDbClient = DynamoDbClient.builder()
            .credentialsProvider(StaticCredentialsProvider.create(awsSessionCredentials))
            .region(Region.of("us-west-2")) // Specify your region
            .build();

    public void updateSkIdTable(ConcurrentMap<String, List<String>> liftRidesMap) {
        int itemCount = 0;
        while (itemCount < 200000) { // todo: multithreading may introduce write throttling
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
