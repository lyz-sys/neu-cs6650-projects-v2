package project3.server.servlets;

import javax.servlet.*;
import javax.servlet.http.*;
import java.io.IOException;
import javax.servlet.annotation.WebServlet;
import java.io.PrintWriter;
import java.io.BufferedReader;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import project3.server.RabbitMQUtil;

import project3.*;

@Slf4j
@WebServlet(name = "SkierServlet", urlPatterns = "/skiers/*")
public class SkierServlet extends HttpServlet {
    @Override
    public void init() throws ServletException {
        RabbitMQUtil.initRMQ();
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        // Check if the request is valid
        LiftRide ride = isRequestValid(req);
        if (ride.isValid()) {
            String pathInfo = req.getPathInfo();
            String[] pathParts = pathInfo.split("/");

            String resortID = pathParts[1];
            String seasonID = pathParts[3];
            String dayID = pathParts[5];
            String skierID = pathParts[7];

            String message = String.join(";", resortID, seasonID, dayID, skierID, String.valueOf(ride.getLiftID()), String.valueOf(ride.getTime()));
            try {
                RabbitMQUtil.sendMessage(message);
            } catch (Exception e) {
                e.printStackTrace();
            } // todo: maybe do it asynchronously

            // Set response content type and write response body if needed
            resp.setContentType("application/json");
            resp.getWriter().write("New lift ride created");
            resp.setStatus(HttpServletResponse.SC_CREATED); // 201 status code for created resource
        } else {
            // URL does not match the expected pattern
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid URL pattern");
        }
    }

    // Check if the URL pattern is correct
    private LiftRide isRequestValid(HttpServletRequest req) {
        LiftRide ride = parseRequestBody(req);
        if (ride == null) {
            ride.isValid = false;
            log.error("Invalid request body");
            return ride;
        }
        ride.isValid = isLiftIDValid(ride.getLiftID()) && isTimeValid(ride.getTime()) && isURLPatternValid(req.getPathInfo());
        return ride;
    }

    private LiftRide parseRequestBody(HttpServletRequest req) {
        LiftRide ride = null;
        try {
            String requestBody = getRequestBody(req);
            if (!requestBody.isEmpty()) {
                Gson gson = new Gson();
                ride = gson.fromJson(requestBody, LiftRide.class);
            }
        } catch (JsonSyntaxException e) {
            log.error("Invalid request body", e);
        }
        return ride;
    }

    private String getRequestBody(HttpServletRequest req) {
        StringBuilder sb = new StringBuilder();
        String line;
        try (BufferedReader reader = req.getReader()) {
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
        } catch (IOException e) {
            log.error("Failed to read request body", e);
        }
        return sb.toString();
    }

    private boolean isLiftIDValid(int liftID) {
        return liftID >= 1 && liftID <= 40;
    }

    private boolean isTimeValid(int time) {
        return time >= 1 && time <= 360;
    }

    private boolean isURLPatternValid(String pathInfo) {
        String[] pathParts = pathInfo.split("/");
        try {
            if (pathParts.length != 8 ||
                    (!"seasons".equals(pathParts[2])) ||
                    (!"days".equals(pathParts[4])) ||
                    (!"skiers".equals(pathParts[6]))) {
                return false;
            }

            int resortID = Integer.parseInt(pathParts[1]);
            if (resortID < 1 || resortID > 10) {
                return false;
            }

            int skierID = Integer.parseInt(pathParts[7]);
            if (skierID < 1 || skierID > 100000) {
                return false;
            }

            int dayID = Integer.parseInt(pathParts[5]);
            if (dayID < 1 || dayID > 366) {
                return false;
            }
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }

    @Override
    public void destroy() {
        RabbitMQUtil.destroyRMQ();
    }

    static class LiftRide {
        private int time;
        private int liftID;
        private boolean isValid;

        public int getTime() {
            return time;
        }

        public int getLiftID() {
            return liftID;
        }

        public boolean isValid() {
            return isValid;
        }
    }
}
