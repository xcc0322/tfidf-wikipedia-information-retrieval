import java.io.*;
import java.net.*;
import java.util.StringTokenizer;

import javax.xml.stream.*;

public class XmlParserTest {
	public static void main(String[] args) throws Exception {
		long begintime = System.currentTimeMillis();
		String outputname = "D:/hadoop2/wiki-input";
		int no = 1;
		int count = 0;
		FileOutputStream out = new FileOutputStream(outputname + String.format("%04d", no));
		InputStream in = new FileInputStream("D:/enwiki-20131001-pages-articles-multistream.xml/enwiki-20131001-pages-articles-multistream.xml");
		
		XMLInputFactory factory = XMLInputFactory.newInstance();
		XMLStreamReader parser = factory.createXMLStreamReader(in, "windows-1252");
		StringBuilder result = new StringBuilder();
		while (parser.hasNext()) {
			int event = parser.next();
			if (event == XMLStreamConstants.START_ELEMENT) {
				String tagname= parser.getLocalName();
				//extract content
				if (tagname.equals("text")) {
					StringBuilder content = new StringBuilder();
					while(true) {
						if(parser.next() == XMLStreamConstants.END_ELEMENT) break;
						content.append(parser.getText());
					}
					result.append(content.toString().toLowerCase().replaceAll("[\n]|#redirect|\\{\\{[^\\}]+\\}\\}", ""));
					result.append("\n");
					count += content.length();
					if ((count & (1<<24)) > 0) {
						count = 0;
						out.write(result.toString().getBytes());
						out.close();
						result = new StringBuilder();
						no = no + 1;
						out = new FileOutputStream(outputname + String.format("%04d", no));
					}
				}
				//extract title
				else if (tagname.equals("title")) {
					parser.next();
				    String title = parser.getText();
					result.append("<title>" + title + "</title>");
				}
			}
		}
		out.write(result.toString().getBytes());
		out.close();
		long endtime=System.currentTimeMillis();

		long costTime = (endtime - begintime);
		System.out.printf("Total time: %f ms.", (double)costTime/1000);
	}
}