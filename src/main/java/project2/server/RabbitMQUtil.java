package project2.server;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;

import project2.tools.Configuration;

@Slf4j
public class RabbitMQUtil {
    private static final Configuration CONFIG = new Configuration();
    private static final String QUEUE_NAME = CONFIG.getRmqMainQueueName();

    private RabbitMQUtil() {
        throw new IllegalStateException("Utility class");
    }

    public static void sendMessage(String message) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(CONFIG.getRmqHostAddress());
        factory.setUsername(CONFIG.getRmqUsername());
        factory.setPassword(CONFIG.getRmqPassword());
        try (Connection connection = factory.newConnection();
                Channel channel = connection.createChannel()) {
            channel.queueDeclare(QUEUE_NAME, false, false, false, null); // tip: the cnannel parameters must be same for both producer and consumer
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
        }
    }
}
