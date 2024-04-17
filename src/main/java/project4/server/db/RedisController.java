package project4.server.db;

import redis.clients.jedis.Jedis;
import lombok.extern.slf4j.Slf4j;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Arrays;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

@Slf4j
public class RedisController {
    private static final int TOTAL_ITEMS = 200000;
    private static final int FIXED_THREAD_POOL_SIZE = 500;
    private ExecutorService executor = Executors.newFixedThreadPool(FIXED_THREAD_POOL_SIZE);
    private AtomicInteger itemCount = new AtomicInteger(0);
    private JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), "54.201.229.216", 6379);

    public void doSkierPost(BlockingQueue<Map.Entry<String, List<String>>> queue) {
        while (itemCount.get() < TOTAL_ITEMS) {
            try {
                Map.Entry<String, List<String>> entry = queue.take(); // Block until an item is available
                executor.submit(() -> {
                    try (Jedis jedis = jedisPool.getResource()) {
                        // Process the entry using the fetched Jedis instance
                        processEntry(jedis, entry);
                    } catch (Exception e) {
                        log.error("Failed to process entry", e);
                    } finally {
                        itemCount.incrementAndGet();
                    }
                });
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Thread was interrupted", e);
                break;
            }

            if (itemCount.get() % 1000 == 0) {
                log.info("Processed {} entries", itemCount.get());
            }
        }

        executor.shutdown(); // todo: seems not reaching this line
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            log.info("Processed {} entries", itemCount.get());
        }
    }

    private void processEntry(Jedis jedis, Map.Entry<String, List<String>> entry) {
        String skierId = entry.getKey();
        List<String> rides = entry.getValue();

        String resortID = rides.get(0);
        String seasonID = rides.get(1);
        String dayID = rides.get(2);
        String liftID = rides.get(3);
        int verticals = 10 * Integer.parseInt(liftID);

        // Start pipelining
        Pipeline pipeline = jedis.pipelined();

        // Store or update the ride data
        String key = resortID + "_" + seasonID + "_" + dayID;
        pipeline.hincrBy(key, skierId, verticals);

        // Add skierId to a set to track unique skiers and increment the count if the
        // skier was not already in the set
        String uniqueSkiersKey = key + "_unique";
        Response<Long> isNewSkier = pipeline.sadd(uniqueSkiersKey, skierId);

        // Execute all commands in the pipeline and get the responses
        pipeline.syncAndReturnAll();

        // Only increment unique skier count if the skier was added to the set
        if (isNewSkier.get() == 1) {
            String uniqueSkierCountKey = "unique_count_" + key;
            jedis.incr(uniqueSkierCountKey);
        }
    }

    public Integer getTotalVerticalForSkier(String skierId, String resortId, List<String> seasonIds) { // todo:
                                                                                                       // multithreading
        try (Jedis jedis = jedisPool.getResource()) {
            int totalVertical = 0;

            // Define known dayIds
            List<String> knownDayIds = Arrays.asList("1", "2", "3");

            for (String seasonId : seasonIds) {
                for (String dayId : knownDayIds) {
                    // Construct the key for each known day
                    String key = resortId + "_" + seasonId + "_" + dayId;

                    // Get the verticals for the skier from the hash for that day
                    String verticalsString = jedis.hget(key, skierId);
                    totalVertical += verticalsString != null ? Integer.parseInt(verticalsString) : 0;
                }
            }
            return totalVertical;
        }
    }

    public Integer getSkiDayVerticalForSkier(String resortId, String seasonId, String dayId,
            String skierId) {
        try (Jedis jedis = jedisPool.getResource()) {
            // The key consists of the resortId, seasonId, and dayId
            String key = resortId + "_" + seasonId + "_" + dayId;
            // The skierId is a field within the hash
            String verticalsString = jedis.hget(key, skierId);
            return verticalsString != null ? Integer.parseInt(verticalsString) : 0;
        }
    }

    public Integer getUniqueSkierCount(String resortId, String seasonId, String dayId) {
        try (Jedis jedis = jedisPool.getResource()) {
            // The unique skiers count key is constructed similarly to the POST method
            String uniqueSkiersKey = "unique_count_" + resortId + "_" + seasonId + "_" + dayId;
            // Retrieve the unique skiers count using the get command, not hget
            String countString = jedis.get(uniqueSkiersKey);
            return countString != null ? Integer.parseInt(countString) : 0;
        }
    }
}