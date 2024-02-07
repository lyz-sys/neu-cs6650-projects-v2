package server;

import javax.servlet.*;
import javax.servlet.http.*;
import java.io.IOException;
import javax.servlet.annotation.WebServlet;
import java.io.PrintWriter;

@WebServlet(name = "SkierServlet", urlPatterns = "/skiers/*")
public class SkierServlet extends HttpServlet {
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String pathInfo = req.getPathInfo();
        String[] pathParts = pathInfo.split("/");

        // Check if the URL pattern is correct
        if (isPostURLValid(pathParts)) {
            String resortID = pathParts[1]; // todo: later turn into correct type
            String seasonID = pathParts[3];
            String dayID = pathParts[5];
            String skierID = pathParts[7];

            // Do something with the parameters, like storing the lift ride for the skier
            // ...

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
    private boolean isPostURLValid(String[] pathParts) {
        try {
            if (pathParts.length == 8 && 
                "seasons".equals(pathParts[2]) && 
                "days".equals(pathParts[4]) && 
                "skiers".equals(pathParts[6])) {
                Integer.parseInt(pathParts[1]);
                Integer.parseInt(pathParts[7]);
                int dayID = Integer.parseInt(pathParts[5]);
                if (dayID >= 1 && dayID <= 366) {
                    return true;
                }
            }
            return false;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
