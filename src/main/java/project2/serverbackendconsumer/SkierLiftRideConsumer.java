package project2.serverbackendconsumer;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;

import project2.tools.Configuration;

@Slf4j
public class SkierLiftRideConsumer {
    private static final Configuration CONFIG = new Configuration();
    private static final String QUEUE_NAME = CONFIG.getRmqMainQueueName();
    private static final ConcurrentHashMap<String, Integer> liftRidesMap = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(CONFIG.getRmqHostAddress()); // Your RabbitMQ server address
        factory.setUsername(CONFIG.getRmqUsername()); // Uncomment if necessary
        factory.setPassword(CONFIG.getRmqPassword()); // Uncomment if necessary

        for (int i = 0; i < Runtime.getRuntime().availableProcessors(); i++) {
            new Thread(() -> {
                try {
                    final Connection connection = factory.newConnection();
                    final Channel channel = connection.createChannel();
                    channel.queueDeclare(QUEUE_NAME, true, false, false, null);

                    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                        String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                        String skierId = extractSkierId(message); // Implement this method based on your message format
                        liftRidesMap.merge(skierId, 1, Integer::sum); // Increment the count for the skier
                        log.info("Received and recorded message: " + message);
                    };

                    channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });
                } catch (IOException | TimeoutException e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }

    private static String extractSkierId(String message) {
        // Your logic to extract skier ID from the message
        return message.split(",")[0]; // Example placeholder
    }
}
