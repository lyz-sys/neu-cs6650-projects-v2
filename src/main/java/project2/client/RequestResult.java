package project2.client;

public record RequestResult(int statusCode, long latency, long startTime, boolean successful) {
}
