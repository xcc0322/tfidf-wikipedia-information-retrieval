
package wtfidf;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;  
import java.util.ArrayList;  
import java.util.Collections;
  





import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.hbase.HBaseConfiguration;  
import org.apache.hadoop.hbase.KeyValue;  
import org.apache.hadoop.hbase.client.HTable;  
import org.apache.hadoop.hbase.client.Result;  
import org.apache.hadoop.hbase.client.ResultScanner;  
import org.apache.hadoop.hbase.client.Scan;  

class KeyValuePair implements Comparable<KeyValuePair>{
	double y;
	String x;
	KeyValuePair(String x, String y) {
		this.x = new String(x);

		if(y.equals("Infinity")) this.y = 0;
		else this.y = Double.valueOf(y);
	}
	@Override
	public int compareTo(KeyValuePair arg0) {
		if (this.y > arg0.y) return -1;
		if(this.y < arg0.y) return 1;
		return 0;
	}
}


public class HbaseTest {  

	public static String indexByDocTableName = "doc_word_table_quick";
	public static String indexByWordTableName = "word_doc_table_quick";
	public static String indexByDocHtml = "F:\\Eclipse_Workspace\\WikiTfidf\\WebContent\\result.html";
	public static String indexByWordHtml = "F:\\Eclipse_Workspace\\WikiTfidf\\WebContent\\result.html";
    public static Configuration configuration;  
    static {  
        configuration = HBaseConfiguration.create();  
        configuration.set("hbase.zookeeper.property.clientPort", "2181");  
        configuration.set("hbase.zookeeper.quorum", "10.141.210.12, 10.141.210.13, 10.141.210.14, 10.141.210.15");  
        configuration.set("hbase.master", "10.141.210.11:6000");
    }
    public static String TopK(String key, int K, boolean indexByDoc) throws IOException{
    	String tableName = indexByDoc?indexByDocTableName:indexByWordTableName;
        ArrayList<KeyValuePair> list = new ArrayList<KeyValuePair>();
        HTable table = new HTable(configuration, tableName);
		StringBuilder resultList = new StringBuilder();
        
        ResultScanner rs = table.getScanner(
        	new Scan((key + ",").getBytes(), (key + "-").getBytes()));
        for (Result r : rs) {
        	String row = new String(r.getRow());
            //System.out.println("»ñµÃµ½rowkey:" + row);  
            for (KeyValue keyValue : r.raw()) {  
            	String num = new String(keyValue.getValue());
                if(Double.valueOf(num)!= Double.valueOf(num)+1){
                	list.add(new KeyValuePair(row, num));
                	System.out.println(row + "--" + num );
                }
            }
        }
        table.close();
        System.out.println("Done Fetching");
        if (K > list.size()) K = list.size();
        System.out.println(K);
        Collections.sort(list);
        for(int i=0; i<K; i++) {
        	KeyValuePair result = list.get(i);
        	int commaIndex = (result.x).indexOf(',');
        	String s = result.x.substring(commaIndex + 1);
        	if (!indexByDoc) s = String.format(
    			"<a href=\"http://en.wikipedia.org/wiki/%s\">%s</a><br>",
    			s,
    			s.replace('_', ' ')
        	);
        	else s = s + "<br>";
        	resultList.append(s);
        	System.out.println(result.x + "-----" + result.y);
        }
        File output = new File(indexByDoc?indexByDocHtml:indexByWordHtml);
		StringBuilder sb = new StringBuilder();
        FileReader fr = new FileReader(output);
		int c;
		while ((c = fr.read()) != -1) {
			sb.append((char) c);
		}
		fr.close();
		String strResponse = String.format(sb.toString(),key, resultList.toString());
        return strResponse;
    }
}

