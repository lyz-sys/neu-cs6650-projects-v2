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
import java.util.concurrent.CountDownLatch;

/**
 * Driver class for Part 1 of Project 1.
 */
public class ThroughputEstimationDriver {
    private static final Logger logger = LogManager.getLogger(ThroughputEstimationDriver.class);
    private static final int MAX_EVENTS = 100;
    private static BlockingQueue<SkierLiftRideEvent> eventQueue = new LinkedBlockingQueue<>(MAX_EVENTS);
    private static BlockingQueue<SkierLiftRideEvent> receiveQueue = new LinkedBlockingQueue<>(MAX_EVENTS);

    public static void main(String[] args) {
        // Generate events
        for (int i = 0; i < MAX_EVENTS; i++) {
            SkierLiftRideEvent event = new SkierLiftRideEvent();
            eventQueue.offer(event); // Non-blocking operation
        }

        long startTime = System.currentTimeMillis();
        int numOfRequests = MAX_EVENTS;
        CountDownLatch latch1 = new CountDownLatch(1);
        new Thread(new ThroughputEstimationTask(numOfRequests, eventQueue, latch1, receiveQueue)).start();

        // Block here for a few seconds to let some events be processed
        int waitTimeInSeconds = 4; 
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(waitTimeInSeconds));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // set the interrupt flag
            logger.error("Thread interrupted", e);
        }

        long elapsed = System.currentTimeMillis() - startTime; // Recalculate elapsed time after the sleep
        double outputRate = (MAX_EVENTS - eventQueue.size()) / (elapsed / 1000.0);
        double inputRate = receiveQueue.size() / (elapsed / 1000.0);

        try {
            latch1.await(); // Wait for at least one thread to finish
        } catch (InterruptedException e) {
            // Rethrow the InterruptedException
            Thread.currentThread().interrupt();
        }

        logger.info("Throughput estimation completed.");
        logger.info("request exit rate of client system: {} requests/sec", outputRate);
        logger.info("request arrival rate of client system: {} requests/sec", inputRate);
    }
}