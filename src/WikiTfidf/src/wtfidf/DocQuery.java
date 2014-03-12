
package wtfidf;
import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.PrintWriter;
public class DocQuery extends HttpServlet {
	private static final long serialVersionUID = 1L;
    public DocQuery() {
        super();
    }

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		response.setContentType("text/html");
		PrintWriter pw = response.getWriter();
		pw.println("");
		pw.println("<h1>Hello World</h1>");
		pw.println("");
	}

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String word = (String) request.getParameter("doc");
		String kString = (String) request.getParameter("k");
		int k = Integer.parseInt(kString);

		String strResponse;
		strResponse = HbaseTest.TopK(word.replace(' ', '_'), k, true).toString();
		PrintWriter out = null;
		try {
			out = response.getWriter();
			out.println(strResponse);
			out.flush();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (out != null) {
				out.close();
			}
		}
	}

}
