package project2.tools;

import java.io.InputStream;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import lombok.Data;

@Slf4j
@Data
public class Configuration {
    private String basePath;
    private int connectionTimeout;
    private int maxEvents;
    private int threadNum;
    private int requestNumPerThread;

    public Configuration() {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                log.info("Sorry, unable to find config.properties");
                return;
            }
            Properties prop = new Properties();
            prop.load(input);
            parseProperties(prop);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void parseProperties(Properties prop) {
        basePath = prop.getProperty("base.path");

        String connectionTimeoutStr = prop.getProperty("connection.timeout");
        try {
            connectionTimeout = Integer.parseInt(connectionTimeoutStr);
        } catch (NumberFormatException e) {
            log.error("Error parsing connectionTimeout from string: " + e.getMessage());
        }

        String maxEventsStr = prop.getProperty("client.max.event");
        try {
            maxEvents = Integer.parseInt(maxEventsStr);
        } catch (NumberFormatException e) {
            log.error("Error parsing maxEvents from string: " + e.getMessage());
        }

        String threadNumStr = prop.getProperty("client.thread.num");
        try {
            threadNum = Integer.parseInt(threadNumStr);
        } catch (NumberFormatException e) {
            log.error("Error parsing threadNum from string: " + e.getMessage());
        }

        String requestNumPerThreadStr = prop.getProperty("client.request.num.per.thread");
        try {
            requestNumPerThread = Integer.parseInt(requestNumPerThreadStr);
        } catch (NumberFormatException e) {
            log.error("Error parsing requestNumPerThread from string: " + e.getMessage());
        }
    }
}
