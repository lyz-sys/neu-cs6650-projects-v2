package project2.serverbackendconsumer;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;

import project2.tools.Configuration;

@Slf4j
public class Driver {
    private static final Configuration CONFIG = new Configuration(); // todo: try rmq util?
    private static final String QUEUE_NAME = CONFIG.getRmqMainQueueName();
    private static final ConcurrentHashMap<String, List<String>> liftRidesMap = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(CONFIG.getRmqHostAddress());
        factory.setUsername(CONFIG.getRmqUsername());
        factory.setPassword(CONFIG.getRmqPassword());
        for (int i = 0; i < CONFIG.getConsumerThreadNum(); i++) {
            new Thread(() -> {
                try {
                    final Connection connection = factory.newConnection();
                    final Channel channel = connection.createChannel();
                    channel.queueDeclare(QUEUE_NAME, false, false, false, null); // tip: the cnannel parameters must be
                                                                                 // same for both producer and consumer

                    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                        String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                        processMessage(message);
                    };

                    channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {
                    });
                } catch (IOException | TimeoutException e) {
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
