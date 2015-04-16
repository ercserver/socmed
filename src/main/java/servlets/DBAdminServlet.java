package servlets;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.sql.ResultSetMetaData;


@WebServlet(name = "DBAdminServlet")
public class DBAdminServlet extends HttpServlet {
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        System.out.print("here");
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

    }

    private int dumpData(java.sql.ResultSet rs, java.io.PrintWriter out)
            throws Exception {
        int rowCount = 0;

        out.println("<P ALIGN='center'><TABLE BORDER=1>");
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        // table header
        out.println("<TR>");
        for (int i = 0; i < columnCount; i++) {
            out.println("<TH>" + rsmd.getColumnLabel(i + 1) + "</TH>");
        }
        out.println("</TR>");
        // the data
        while (rs.next()) {
            rowCount++;
            out.println("<TR>");
            for (int i = 0; i < columnCount; i++) {
                out.println("<TD>" + rs.getString(i + 1) + "</TD>");
            }
            out.println("</TR>");
        }
        out.println("</TABLE></P>");
        return rowCount;
    }
}
