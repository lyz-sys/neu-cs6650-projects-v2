package project4.server.servlets;

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
import project4.server.db.DynamoDBController;
import project4.server.db.RedisController;
import project4.server.RabbitMQUtil;

import project4.*;
import java.util.List;

@Slf4j
@WebServlet(name = "SkierServlet", urlPatterns = "/skiers/*")
public class SkierServlet extends HttpServlet {
    // private DynamoDBController dynamoDBController = new DynamoDBController();
    private RedisController redisController = new RedisController();
    private final Gson gson = new Gson();

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

            String message = String.join(";", resortID, seasonID, dayID, skierID, String.valueOf(ride.getLiftID()),
                    String.valueOf(ride.getTime()));
            try {
                RabbitMQUtil.sendMessage(message);
            } catch (Exception e) {
                e.printStackTrace();
            }

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
    private LiftRide isRequestValid(HttpServletRequest req) throws IOException {
        LiftRide ride = parseRequestBody(req);
        if (ride == null) {
            ride.isValid = false;
            log.error("Invalid request body");
            return ride;
        }
        ride.isValid = isLiftIDValid(ride.getLiftID()) && isTimeValid(ride.getTime())
                && isURLPatternValid(req.getPathInfo());
        return ride;
    }

    private LiftRide parseRequestBody(HttpServletRequest req) throws IOException {
        return gson.fromJson(req.getReader(), LiftRide.class);
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String pathInfo = request.getPathInfo();
        if (pathInfo != null) {
            String[] pathParts = pathInfo.split("/");
            if (pathParts.length == 3 && "vertical".equals(pathParts[2])) {
                processGetVerticals(pathParts[1], parseVerticalBody(request), response);
            } else {
                processGetAVertical(pathParts[1], pathParts[3], pathParts[5], pathParts[7], response);
            }
        } else {
            // Handle request without additional path info
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
        }
    }

    private void processGetAVertical(String resortID, String seasonID, String dayID, String skierID,
            HttpServletResponse response) throws IOException {
        // Integer aVertical = dynamoDBController.getSkiDayVerticalForSkier(resortID, seasonID, dayID, skierID);
        Integer aVertical = redisController.getSkiDayVerticalForSkier(resortID, seasonID, dayID, skierID);
        
        if (aVertical == 0) {
            // No item found
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
        } else {
            // Set response content type and write response body
            response.setContentType("application/json");
            response.getWriter().write(String.format("{\"skierID\": %s, \"the skier's vertical\": %s}", skierID, aVertical));
        }
    }

    private void processGetVerticals(String skierID, VerticalBody verticalBody, HttpServletResponse response)
            throws IOException {
        // Integer verticals = dynamoDBController.getTotalVerticalForSkier(skierID, verticalBody.getResort(), verticalBody.getSeasons());
        Integer verticals = redisController.getTotalVerticalForSkier(skierID, verticalBody.getResort(), verticalBody.getSeasons());

        if (verticals == 0) {
            // No item found
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
        } else {
            // Set response content type and write response body
            response.setContentType("application/json");
            response.getWriter().write(String.format("{\"skierID\": %s, \"totalVert\": %s}", skierID, verticals));
        }
    }

    private VerticalBody parseVerticalBody(HttpServletRequest req) throws IOException {
        return gson.fromJson(req.getReader(), VerticalBody.class);
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

    public static class LiftRide {
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

    public static class VerticalBody {
        private String resort;
        private List<String> seasons;

        public String getResort() {
            return resort;
        }

        public List<String> getSeasons() {
            return seasons;
        }
    }
}
