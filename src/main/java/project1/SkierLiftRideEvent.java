package project1;

import java.util.concurrent.ThreadLocalRandom;
import io.swagger.client.model.LiftRide;

public class SkierLiftRideEvent {
    private int skierID;
    private int resortID;
    private int liftID;
    private int time;
    private LiftRide body;
    private String seasonID = "2024";
    private String dayID = "1";

    public SkierLiftRideEvent() {
        this.skierID = ThreadLocalRandom.current().nextInt(1, 100001);
        this.resortID = ThreadLocalRandom.current().nextInt(1, 11);
        this.liftID = ThreadLocalRandom.current().nextInt(1, 41);
        this.time = ThreadLocalRandom.current().nextInt(1, 361);
        this.body = new LiftRide();
        body.setTime(time);
        body.setLiftID(liftID);
    }

    // Getters and setters for each field if needed

    public int getSkierID() {
        return skierID;
    }

    public int getResortID() {
        return resortID;
    }

    public String getSeasonID() {
        return seasonID;
    }

    public String getDayID() {
        return dayID;
    }

    public LiftRide getBody() {
        return body;
    }

    @Override
    public String toString() {
        return "SkierLiftRideEvent{" +
                "skierID=" + skierID +
                ", resortID=" + resortID +
                ", liftID=" + liftID +
                ", time=" + time +
                ", body=" + body +
                ", seasonID='" + seasonID + '\'' +
                ", dayID='" + dayID + '\'' +
                '}';
    }
}
