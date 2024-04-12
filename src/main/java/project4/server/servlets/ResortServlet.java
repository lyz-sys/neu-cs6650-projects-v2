package project4.server.servlets;

import javax.servlet.*;
import javax.servlet.http.*;
import java.io.IOException;
import javax.servlet.annotation.WebServlet;
import project4.server.db.DynamoDBController;

@WebServlet(name = "ResortServlet", urlPatterns = "/resorts/*")
public class ResortServlet extends HttpServlet {
    private DynamoDBController dynamoDBController = new DynamoDBController();

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        String pathInfo = request.getPathInfo();
        if (pathInfo != null) {
            String[] pathParts = pathInfo.split("/");
            processGetUniqueSkier(pathParts[1], pathParts[3], pathParts[5], response);
        } else {
            // Handle request without additional path info
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
        }
    }

    private void processGetUniqueSkier(String resortID, String seasonID, String dayID,
            HttpServletResponse response) throws IOException {
        Integer uniqueSkier = dynamoDBController.getUniqueSkierCount(resortID, seasonID, dayID);

        if (uniqueSkier == 0) {
            // No item found
            response.sendError(HttpServletResponse.SC_NOT_FOUND);
        } else {
            // Set response content type and write response body
            response.setContentType("application/json");
            response.getWriter().write(String.format("{\"number of unique skier is\": %s", uniqueSkier));
        }
    }
}