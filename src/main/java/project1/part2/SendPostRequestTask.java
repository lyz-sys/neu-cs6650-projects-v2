package project1.part2;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import io.swagger.client.api.SkiersApi;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.swagger.client.ApiException;
import io.swagger.client.ApiResponse;
import io.swagger.client.ApiClient;
import project1.SkierLiftRideEvent;
import project1.Configuration;

public class SendPostRequestTask implements Runnable {
    private static final Logger logger = LogManager.getLogger(SendPostRequestTask.class);
    private int numOfRequests;
    private BlockingQueue<SkierLiftRideEvent> eventQueue;
    private List<RequestResult> requestResultList = new ArrayList<>();
    private Configuration config;
    private final SkiersApi api;

    public SendPostRequestTask(int numOfRequests, BlockingQueue<SkierLiftRideEvent> eventQueue) {
        this.numOfRequests = numOfRequests;
        this.eventQueue = eventQueue;
        config = new Configuration();
        api = new SkiersApi(
            new ApiClient().setConnectTimeout(config.getConnectionTimeout()).setBasePath(config.getBasePath()));
    }

    public List<RequestResult> getRequestResultList() {
        return requestResultList;
    }

    @Override
    public void run() {
        for (int i = 0; i < numOfRequests; i++) {
            processEvent();
        }
    }

    private void processEvent() {
        SkierLiftRideEvent event = eventQueue.poll();

        if (event == null) {
            handleEmptyQueue();
            return;
        }

        requestResultList.add(sendRequestsUntilSuccessful(event));
    }

    private RequestResult sendRequestsUntilSuccessful(SkierLiftRideEvent event) {
        RequestResult result = null;
        for (int j = 0; j < 5; j++) {
            result = attemptPostRequest(api, event);
            if (result.isSuccessful()) {
                break;
            }
        }
        return result;
    }

    private void handleEmptyQueue() {
        logger.info("not enough events in the queue");
        try {
            Thread.sleep(100); // Wait for a while before retrying
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore the interrupted status
        }
    }

    private RequestResult attemptPostRequest(SkiersApi api, SkierLiftRideEvent event) {
        long startTime = System.currentTimeMillis();
        try {
            ApiResponse<Void> response = api.writeNewLiftRideWithHttpInfo(
                    event.getBody(), event.getResortID(), event.getSeasonID(), event.getDayID(), event.getSkierID());
            long latency = System.currentTimeMillis() - startTime;
            return new RequestResult(response.getStatusCode(), latency, startTime, true);
        } catch (ApiException e) {
            long latency = System.currentTimeMillis() - startTime;
            handleApiException(e);
            return new RequestResult(e.getCode(), latency, startTime, false);
        }
    }

    private static void handleApiException(ApiException e) {
        logger.error("API Exception occurred: " + e.getMessage());
        logger.info(e.getCode());
    }
}