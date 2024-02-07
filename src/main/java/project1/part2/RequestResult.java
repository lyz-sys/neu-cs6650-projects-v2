package project1.part2;

public class RequestResult {
    private final int statusCode;
    private final long latency; // in milliseconds
    private final long startTime;
    private final boolean successful;

    public RequestResult(int statusCode, long latency, long startTime, boolean successful) {
        this.statusCode = statusCode;
        this.latency = latency;
        this.startTime = startTime;
        this.successful = successful;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public long getLatency() {
        return latency;
    }

    public long getStartTime() {
        return startTime;
    }

    public boolean isSuccessful() {
        return successful;
    }
}
