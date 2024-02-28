package project2.serverbackendconsumer;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import project2.server.RabbitMQUtil;
import project2.tools.Configuration;

@Slf4j
public class Driver {
    private static final Configuration CONFIG = new Configuration();
    private static final ConcurrentHashMap<String, List<String>> liftRidesMap = new ConcurrentHashMap<>();

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
    }

    private static void processMessage(String message) {
        String[] parts = message.split(";");

        List<String> liftRides = new ArrayList<>();
        liftRides.add(parts[0]);
        liftRides.add(parts[1]);
        liftRides.add(parts[2]);
        String skierId = parts[3];
        
        liftRidesMap.putIfAbsent(skierId, liftRides);
        log.info("map size now: " + liftRidesMap.size());
    }
}
