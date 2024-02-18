package project2.client;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Collections;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Util
 */
@Slf4j
public class Util {  
    private Util() {
        throw new IllegalStateException("Utility class");
    }

    public static void writeToCsv(List<RequestResult> requestResults) {
        log.info("Writing to CSV");
        try (PrintWriter writer = new PrintWriter(new File("./logs/results.csv"))) {
            StringBuilder sb = new StringBuilder();
            sb.append("StartTime,RequestType,Latency,ResponseCode\n");
    
            for (RequestResult result : requestResults) {
                sb.append(result.startTime()).append(",");
                sb.append("POST").append(",");
                sb.append(result.latency()).append(",");
                sb.append(result.statusCode()).append("\n");
            }
    
            writer.write(sb.toString());
        } catch (FileNotFoundException e) {
            log.info(e.getMessage());
        }
    }    

    public static void calculateStatistics(List<RequestResult> requestResults, long wallTimeSeconds) {
        log.info("Starting to calculate statistics.");

        if (requestResults.isEmpty()) {
            log.info("No data available.");
            return;
        }

        int successfulCount = 0;
        for (RequestResult result : requestResults) {
            if (result.successful()) {
                successfulCount++;
            }
        }

        // Convert latencies to a list of longs for easier processing
        List<Long> latencies = requestResults.stream()
                .map(RequestResult::latency)
                .collect(Collectors.toList());

        // Calculate mean
        long mean = (long) latencies.stream()
                .mapToLong(Long::longValue)
                .average()
                .orElse(0.0);

        // Sort for median and percentile calculations
        Collections.sort(latencies);
        long median = latencies.get(latencies.size() / 2);
        if (latencies.size() % 2 == 0) {
            median = (median + latencies.get(latencies.size() / 2 - 1)) / 2;
        }

        // Calculate p99
        long p99Index = (long) Math.ceil(0.99 * latencies.size()) - 1;
        long p99 = latencies.get((int) p99Index);

        // Min and Max
        long min = latencies.get(0);
        long max = latencies.get(latencies.size() - 1);

        // Output
        log.info("Successful requests: " + successfulCount);
        log.info("Mean response time: " + mean + "ms");
        log.info("Median response time: " + median + "ms");
        log.info("Throughput: " + requestResults.size() / (wallTimeSeconds / 1000) + " requests/sec");
        log.info("p99 response time: " + p99 + "ms");
        log.info("Min response time: " + min + "ms");
        log.info("Max response time: " + max + "ms");
    }
}