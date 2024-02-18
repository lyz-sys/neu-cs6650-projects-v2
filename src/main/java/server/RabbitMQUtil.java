package server;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RabbitMQUtil {
    private static final String QUEUE_NAME = "my_queue_1"; // The name of the queue to send messages to

    private RabbitMQUtil() {
        throw new IllegalStateException("Utility class");
    }

    public static void sendMessage(String message) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("35.92.143.116"); // Set this to your RabbitMQ server address
        // You can also set other connection properties here (e.g., username, password, port, virtual host)

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
             
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
            log.info(" [x] Sent '" + message + "'");
        }
    }
}
