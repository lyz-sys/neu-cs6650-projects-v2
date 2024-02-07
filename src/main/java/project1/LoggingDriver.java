package project1;

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
import project1.SkierLiftRideEvent;
import java.util.concurrent.ScheduledExecutorService;
import project1.part1.SendPostRequestTask;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import org.apache.logging.log4j.LogManager;
import java.util.Collections;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import java.io.FileOutputStream;

/**
 * Logging driver class for Project 1.
 */
@Slf4j
public class LoggingDriver {
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

        // for logging purposes
        ScheduledExecutorService throughputLogger = Executors.newScheduledThreadPool(1);
        throughputLogger.scheduleAtFixedRate(() -> {
            int localSuccessfulCount = 0;
            for (SendPostRequestTask sendPostRequestTask : sendPostRequestTaskList) {
                localSuccessfulCount += sendPostRequestTask.getSuccessfulCount();
            }
            
            writeToCsv(localSuccessfulCount / ((System.currentTimeMillis() - startTime) /
                    1000));
        }, 0, 1, TimeUnit.SECONDS);

        try {
            executor.shutdown(); // Shutdown the executor service
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

            executor2.shutdown();
            executor2.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

            throughputLogger.shutdown();
            throughputLogger.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException ie) {
            log.info("the executor service was interrupted");
            executor.shutdownNow();
            executor2.shutdownNow();
            throughputLogger.shutdownNow();
            Thread.currentThread().interrupt(); // Preserve interrupt status
        }

        // Wait for all threads to finish and collect results
        for (SendPostRequestTask sendPostRequestTask : sendPostRequestTaskList) {
            successfulCount += sendPostRequestTask.getSuccessfulCount();
            unsuccessfulCount += sendPostRequestTask.getUnsuccessfulCount();
        }

        log.info("Successful requests: {}", successfulCount);
        log.info("Unsuccessful requests: {}", unsuccessfulCount);
        log.info("Total run time: {} sec", (System.currentTimeMillis() - startTime) / 1000);
        log.info("Total throughput: {} requests/sec", successfulCount /
                ((System.currentTimeMillis() - startTime) / 1000));
    }

    private static void writeToCsv(long throughput) {
        try (PrintWriter writer = new PrintWriter(new FileOutputStream(new File("./logs/graph-data.csv"), true))) {
            StringBuilder sb = new StringBuilder();
            sb.append(throughput);
            sb.append('\n');
            writer.write(sb.toString());
        } catch (FileNotFoundException e) {
            log.error("Error writing to CSV", e);
        }
    }
}
