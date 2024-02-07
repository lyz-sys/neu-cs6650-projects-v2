package project1.part2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.Collections;
import java.util.stream.Collectors;

/**
 * Util
 */
public class Util {  
    private static final Logger logger = LogManager.getLogger(Util.class);

    private Util() {
        throw new IllegalStateException("Utility class");
    }

    public static void writeToCsv(List<RequestResult> requestResults) {
        logger.info("Writing to CSV");
        try (PrintWriter writer = new PrintWriter(new File("./logs/results.csv"))) {
            StringBuilder sb = new StringBuilder();
            sb.append("StartTime,RequestType,Latency,ResponseCode\n");
    
            for (RequestResult result : requestResults) {
                sb.append(result.getStartTime()).append(",");
                sb.append("POST").append(",");
                sb.append(result.getLatency()).append(",");
                sb.append(result.getStatusCode()).append("\n");
            }
    
            writer.write(sb.toString());
        } catch (FileNotFoundException e) {
            logger.info(e.getMessage());
        }
    }    

    public static void calculateStatistics(List<RequestResult> requestResults, long wallTimeSeconds) {
        logger.info("Starting to calculate statistics.");

        if (requestResults.isEmpty()) {
            logger.info("No data available.");
            return;
        }

        int successfulCount = 0;
        for (RequestResult result : requestResults) {
            if (result.isSuccessful()) {
                successfulCount++;
            }
        }

        // Convert latencies to a list of longs for easier processing
        List<Long> latencies = requestResults.stream()
                .map(RequestResult::getLatency)
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
        logger.info("Successful requests: " + successfulCount);
        logger.info("Mean response time: " + mean + "ms");
        logger.info("Median response time: " + median + "ms");
        logger.info("Throughput: " + requestResults.size() / (wallTimeSeconds / 1000) + " requests/sec");
        logger.info("p99 response time: " + p99 + "ms");
        logger.info("Min response time: " + min + "ms");
        logger.info("Max response time: " + max + "ms");
    }
}