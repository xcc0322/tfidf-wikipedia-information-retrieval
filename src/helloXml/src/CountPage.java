import java.io.*;
import java.net.*;
import java.util.StringTokenizer;

import javax.xml.stream.*;

public class CountPage {
	public static void main(String[] args) throws Exception {
		long begintime = System.currentTimeMillis();
		InputStream in = new FileInputStream("D:/enwiki-20131001-pages-articles-multistream.xml/enwiki-20131001-pages-articles-multistream.xml");
		int total = 0;
		XMLInputFactory factory = XMLInputFactory.newInstance();
		XMLStreamReader parser = factory.createXMLStreamReader(in, "windows-1252");
		while (parser.hasNext()) {
			int event = parser.next();
			if (event == XMLStreamConstants.START_ELEMENT) {
				String tagname= parser.getLocalName();
				if (tagname.equals("title")) {
					total ++;
					if ((total&(1<<11)-1) == (1<<10)) 
						System.out.println(total);
				}
			}
		}
		System.out.println("Total number of pages!" + total);
		long endtime=System.currentTimeMillis();
		long costTime = (endtime - begintime);
		System.out.printf("Total time for counting: %f ms.", (double)costTime/1000);
	}
}