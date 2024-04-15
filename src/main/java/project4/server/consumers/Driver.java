package project4.server.consumers;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import project4.server.RabbitMQUtil;
import project4.Configuration;
import project4.server.db.DynamoDBController;
import java.util.concurrent.ConcurrentMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.AbstractMap;

@Slf4j
public class Driver {
    private static final Configuration CONFIG = new Configuration();
    private static final ConcurrentMap<String, List<String>> liftRidesMap = new ConcurrentHashMap<>();
    private static final DynamoDBController dynamoDBController = new DynamoDBController();
    private static final BlockingQueue<Map.Entry<String, List<String>>> queue = new LinkedBlockingQueue<>();

    public static void main(String[] args) {
        try {
            RabbitMQUtil.initRMQ();
        } catch (Exception e) {
            log.error("Failed to initialize RabbitMQ", e);
        }
        
        for (int i = 0; i < CONFIG.getConsumerThreadNum(); i++) {
            new Thread(() -> {
                try {
                    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                        String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                        processMessage(message);
                    };
                    RabbitMQUtil.consumeMessage(deliverCallback);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }

        dynamoDBController.updateSkIdTable(queue);
    }

    private static void processMessage(String message) {
        String[] parts = message.split(";");

        List<String> liftRides = new ArrayList<>();
        liftRides.add(parts[0]);
        liftRides.add(parts[1]);
        liftRides.add(parts[2]);
        liftRides.add(parts[4]);
        liftRides.add(parts[5]);
        
        String skierId = parts[3];
        
        // liftRidesMap.putIfAbsent(skierId, liftRides);
        queue.offer(new AbstractMap.SimpleEntry<>(skierId, liftRides));
    }
}
