package project4.server.servlets;

import java.io.*;
import javax.servlet.*;
import javax.servlet.http.*;
import java.io.IOException;
import javax.servlet.annotation.WebServlet;
import java.io.PrintWriter;
import java.io.BufferedReader;

@WebServlet(name = "HealthCheckServlet", urlPatterns = "/health/*")
public class HealthCheckServlet extends HttpServlet {

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        // Perform any custom logic here to verify your application's health
        // ...

        // Respond with HTTP 200 OK if everything is fine
        response.setStatus(HttpServletResponse.SC_OK);
        PrintWriter out = response.getWriter();
        out.println("OK");
    }
}
