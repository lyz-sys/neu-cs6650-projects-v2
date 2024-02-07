package project1.part1;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import io.swagger.client.api.SkiersApi;
import io.swagger.client.ApiException;
import io.swagger.client.ApiClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import project1.SkierLiftRideEvent;

/**
 * Driver class for Part 1 of Project 1.
 */
public class Driver {
    private static final Logger logger = LogManager.getLogger(Driver.class);
    private static final int MAX_EVENTS = 200000;
    private static int successfulCount = 0;
    private static int unsuccessfulCount = 0;
    private static BlockingQueue<SkierLiftRideEvent> eventQueue = new LinkedBlockingQueue<>(MAX_EVENTS);
    private static List<SendPostRequestTask> sendPostRequestTaskList = new ArrayList<>();

    public static void main(String[] args) {
        // Generate 200,000 events
        for (int i = 0; i < MAX_EVENTS; i++) {
            SkierLiftRideEvent event = new SkierLiftRideEvent();
            eventQueue.offer(event); // Non-blocking operation
        }

        long startTime = System.currentTimeMillis();

        int numOfRequest = 1000;
        int numOfThread = 32;
        ExecutorService executor = Executors.newFixedThreadPool(numOfThread);
        for (int i = 0; i < numOfThread; i++) {
            SendPostRequestTask task = new SendPostRequestTask(numOfRequest, eventQueue);
            sendPostRequestTaskList.add(task);
            executor.submit(task);
        }

        numOfRequest = 100;
        numOfThread = 1680;
        ExecutorService executor2 = Executors.newFixedThreadPool(numOfThread);
        for (int i = 0; i < numOfThread; i++) {
            SendPostRequestTask task = new SendPostRequestTask(numOfRequest, eventQueue);
            sendPostRequestTaskList.add(task);
            executor2.submit(task);
        }

        try {
            executor.shutdown(); // Shutdown the executor service
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

            executor2.shutdown();
            executor2.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException ie) {
            logger.info("the executor service was interrupted");
            executor.shutdownNow();
            executor2.shutdownNow();
            Thread.currentThread().interrupt(); // Preserve interrupt status
        }

        long endTime = System.currentTimeMillis() - startTime;

        // Wait for all threads to finish and collect results
        for (SendPostRequestTask sendPostRequestTask : sendPostRequestTaskList) {
            successfulCount += sendPostRequestTask.getSuccessfulCount();
            unsuccessfulCount += sendPostRequestTask.getUnsuccessfulCount();
        }

        logger.info("Successful requests: {}", successfulCount);
        logger.info("Unsuccessful requests: {}", unsuccessfulCount);
        logger.info("Total run time: {} sec", (System.currentTimeMillis() - startTime) / 1000);
        logger.info("Total throughput: {} requests/sec", successfulCount /
                (endTime / 1000));
    }
}
