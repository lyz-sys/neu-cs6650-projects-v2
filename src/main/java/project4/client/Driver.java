package project4.client;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import io.swagger.client.api.SkiersApi;
import io.swagger.client.ApiException;
import io.swagger.client.ApiClient;
import lombok.extern.slf4j.Slf4j;

import project4.*; 

/**
 * Driver class for Part 2 of Project 1.
 */
@Slf4j
public class Driver {
    private static final Configuration CONFIG = new Configuration();
    private static final int MAX_EVENTS = CONFIG.getMaxEvents();
    private static final BlockingQueue<SkierLiftRideEvent> EVENT_QUEUE = new LinkedBlockingQueue<>(MAX_EVENTS);
    private static final List<SendPostRequestTask> SEND_POST_REQUEST_TASK_LIST = new ArrayList<>();
    private static final List<RequestResult> REQUEST_RESULT_LIST = new ArrayList<>();

    public static void main(String[] args) {
        for (int i = 0; i < MAX_EVENTS; i++) {
            SkierLiftRideEvent event = new SkierLiftRideEvent();
            EVENT_QUEUE.offer(event);
        }

        long startTime = System.currentTimeMillis();

        int numOfRequest = CONFIG.getRequestNumPerThread(); 
        ExecutorService executor = Executors.newFixedThreadPool(CONFIG.getThreadNum()); 
        for (int i = 0; i < MAX_EVENTS / numOfRequest; i++) {
            SendPostRequestTask task = new SendPostRequestTask(numOfRequest, EVENT_QUEUE);
            SEND_POST_REQUEST_TASK_LIST.add(task);
            executor.submit(task);
        }

        try {
            executor.shutdown(); // Shutdown the executor service
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException ie) {
            log.info("the executor service was interrupted");
            executor.shutdownNow();
            Thread.currentThread().interrupt(); // Preserve interrupt status
        }

        long timeElapsed = System.currentTimeMillis() - startTime;

        // Wait for all threads to finish and collect results
        for (SendPostRequestTask task : SEND_POST_REQUEST_TASK_LIST) {
            REQUEST_RESULT_LIST.addAll(task.getRequestResultList());
        }

        Util.calculateStatistics(REQUEST_RESULT_LIST, timeElapsed);
        Util.writeToCsv(REQUEST_RESULT_LIST);
    }
}
