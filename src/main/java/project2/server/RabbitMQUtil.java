package project2.server;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import javax.servlet.ServletException;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import project2.tools.Configuration;

@Slf4j
public class RabbitMQUtil {
    private static final Configuration CONFIG = new Configuration(); // todo: maybe a singleton pattern
    private static Connection connection;
    private static ChannelPool channelPool;
    private static final String QUEUE_NAME = CONFIG.getRmqMainQueueName();

    private RabbitMQUtil() {
        throw new IllegalStateException("Utility class");
    }

    public static void initRMQ() throws ServletException {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(CONFIG.getRmqHostAddress());
            factory.setUsername(CONFIG.getRmqUsername());
            factory.setPassword(CONFIG.getRmqPassword());
            connection = factory.newConnection();
            channelPool = new ChannelPool(connection); // Initialize the channel pool
        } catch (IOException | TimeoutException e) {
            throw new ServletException("Failed to establish RabbitMQ connection", e);
        }
    }

    public static void destroyRMQ() {
        try {
            channelPool.close(); // Close all channels in the pool

            if (connection != null) {
                connection.close();
            }
        } catch (IOException | TimeoutException e) {
            log.error("Failed to close RabbitMQ connection", e);
        }
    }

    public static void sendMessage(String message) throws Exception {
        Channel channel = null;
        try {
            channel = channelPool.borrowChannel(); // Borrow a channel from the pool
            
            channel.queueDeclare(QUEUE_NAME, false, false, false, null); // tip: the cnannel parameters must be same for both producer and consumer
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ServletException("Failed to borrow a channel from the pool", e);
        } finally {
            if (channel != null) {
                channelPool.returnChannel(channel); // Return the channel to the pool
            }
        }
    }
}
