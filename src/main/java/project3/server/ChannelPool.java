package project3.server;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import project3.Configuration;

public class ChannelPool {
    private static final Configuration CONFIG = new Configuration();
    private static final int POOL_SIZE = CONFIG.getRmqChannelPoolSize();
    private final BlockingQueue<Channel> pool = new LinkedBlockingQueue<>(POOL_SIZE);
    private final Connection connection;

    public ChannelPool(Connection connection) throws IOException {
        this.connection = connection;
        initializePool();
    }

    private void initializePool() throws IOException {
        for (int i = 0; i < POOL_SIZE; i++) {
            pool.offer(connection.createChannel());
        }
    }

    public Channel borrowChannel() throws InterruptedException {
        return pool.take();
    }

    public void returnChannel(Channel channel) {
        if (channel != null) {
            pool.offer(channel);
        }
    }

    public void close() throws IOException, TimeoutException{
        for (Channel channel : pool) {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
        }
    }    
}
