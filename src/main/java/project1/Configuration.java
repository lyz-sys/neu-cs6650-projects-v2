package project1;

import java.io.InputStream;
import java.util.Properties;

public class Configuration {
    private String basePath;
    private int connectionTimeout;

    public Configuration() {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("config.properties")) {
            Properties prop = new Properties();

            if (input == null) {
                System.out.println("Sorry, unable to find config.properties");
                return;
            }

            prop.load(input);
            basePath = prop.getProperty("base.path");

            String connectionTimeoutStr = prop.getProperty("connection.timeout");
            try {
                // Attempt to parse the string to an int
                connectionTimeout = Integer.parseInt(connectionTimeoutStr);
            } catch (NumberFormatException e) {
                // Handle the case where timeoutString is not a valid integer
                System.err.println("Error parsing connectionTimeout from string: " + e.getMessage());
                // Optionally, set connectionTimeout to a default value or handle the error as
                // needed
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public String getBasePath() {
        return basePath;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }
}
