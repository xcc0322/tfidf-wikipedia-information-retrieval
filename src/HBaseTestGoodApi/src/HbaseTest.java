import java.io.BufferedReader;
import java.io.IOException;  
import java.io.InputStreamReader;
import java.util.ArrayList;  
import java.util.Collections;
import java.util.Comparator;
import java.util.List;  
import java.util.Vector;
  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.hbase.HBaseConfiguration;  
import org.apache.hadoop.hbase.HColumnDescriptor;  
import org.apache.hadoop.hbase.HTableDescriptor;  
import org.apache.hadoop.hbase.KeyValue;  
import org.apache.hadoop.hbase.MasterNotRunningException;  
import org.apache.hadoop.hbase.ZooKeeperConnectionException;  
import org.apache.hadoop.hbase.client.Delete;  
import org.apache.hadoop.hbase.client.Get;  
import org.apache.hadoop.hbase.client.HBaseAdmin;  
import org.apache.hadoop.hbase.client.HTable;  
import org.apache.hadoop.hbase.client.HTablePool;  
import org.apache.hadoop.hbase.client.Put;  
import org.apache.hadoop.hbase.client.Result;  
import org.apache.hadoop.hbase.client.ResultScanner;  
import org.apache.hadoop.hbase.client.Scan;  
import org.apache.hadoop.hbase.filter.Filter;  
import org.apache.hadoop.hbase.filter.FilterList;  
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;  
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;  
import org.apache.hadoop.hbase.util.Bytes;  

class KeyValuePair implements Comparable<KeyValuePair>{
	double y;
	String x;
	KeyValuePair(String x, String y) {
		this.x = new String(x);
		this.y = Double.valueOf(y);
	}
	@Override
	public int compareTo(KeyValuePair arg0) {
		if (this.y > arg0.y) return -11;
		else return 1;
	}
}


public class HbaseTest {  

    public static Configuration configuration;  
    static {  
        configuration = HBaseConfiguration.create();  
        configuration.set("hbase.zookeeper.property.clientPort", "2181");  
        configuration.set("hbase.zookeeper.quorum", "10.141.210.12, 10.141.210.13, 10.141.210.14, 10.141.210.15");  
        configuration.set("hbase.master", "10.141.210.11:6000");  
    }  

    public static void main(String[] args) throws IOException, InterruptedException{  
        //createTable("test_api2");  
        createTable("word_doc_table_3"); 
        createTable("doc_word_table_3");  
        //insertData("test_word");  
        //insertData("doc_word_small1");  
        //QueryAll("test_api"); 
        //QueryAll("word_doc_small_d1"); 
        //QueryAll("word_doc_small1");  
        //QueryAll("test_word");
        //TopK("doc_word_small_d1", "AFC_Ajax", 10);  
        // QueryByCondition2("table");  
        //QueryByCondition3("table");  
        //deleteRow("table","abcdef");  
        //deleteByCondition("table","abcdef");  
    }  
  
    /** 
     * 创建表 
     * @param tableName 
     */  
    public static void createTable(String tableName) {  
        System.out.println("start create table ......");  
        try {  
            HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);  
            if (hBaseAdmin.tableExists(tableName)) {
                hBaseAdmin.disableTable(tableName);  
                hBaseAdmin.deleteTable(tableName);  
                System.out.println(tableName + " is exist,detele....");  
            }  
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);  
            tableDescriptor.addFamily(new HColumnDescriptor("value"));
            hBaseAdmin.createTable(tableDescriptor);  
        } catch (MasterNotRunningException e) {  
            e.printStackTrace();  
        } catch (ZooKeeperConnectionException e) {  
            e.printStackTrace();  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
        System.out.println("end create table ......");  
    }  
  
    /** 
     * 插入数据 
     * @param tableName 
     */  
    public static void insertData(String tableName) throws IOException{  
        System.out.println("start insert data ......");  
        HTable table = new HTable(configuration, tableName);
        
        String[] ss = {"a", "b", "c", "d"};
        double[] dd = {1.0,3.0,2,4.5};
        for(int i=0; i<4; i++) {
        	Put put = new Put(("ABC," + ss[i]).getBytes());
        	put.add("value".getBytes(), null, Double.valueOf(dd[i]).toString().getBytes());
        	table.put(put);
        }
        
        System.out.println("end insert data ......");  
    }  

    /** 
     * 查询所有数据 
     * @param tableName 
     */  
    public static void QueryAll(String tableName) {  
        HTable table = null;
        try {  
			table = new HTable(configuration, tableName);
            ResultScanner rs = table.getScanner(new Scan());  
            for (Result r : rs) {  
                System.out.println("获得到rowkey:" + new String(r.getRow()));  
                for (KeyValue keyValue : r.raw()) {  
                    System.out.println("列：" + new String(keyValue.getFamily())  
                            + "====值:" + new String(keyValue.getValue()));  
                }  
            }  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
    }
    
    /** 
     * RangeQuery
     * @param tableName 
     */  
    public static void RangeQuery(String tableName) {  
        HTable table = null;
        try {  
			table = new HTable(configuration, tableName);
            ResultScanner rs = table.getScanner(new Scan("ABC,".getBytes(), "ABC-".getBytes()));  
            for (Result r : rs) {  
                System.out.println("获得到rowkey:" + new String(r.getRow()));  
                for (KeyValue keyValue : r.raw()) {  
                    System.out.println("列：" + new String(keyValue.getFamily())  
                            + "====值:" + new String(keyValue.getValue()));  
                }
            }
        } catch (IOException e) {  
            e.printStackTrace();  
        }
    }

    /** 
     * RangeQuery
     * @param tableName 
     * @throws InterruptedException 
     */  
    public static void TopK(String tableName, String key, int K) throws IOException, InterruptedException{  
        HTable table = null;
        ArrayList<KeyValuePair> list = new ArrayList<KeyValuePair>();

		table = new HTable(configuration, tableName);
        ResultScanner rs = table.getScanner(new Scan((key + ",").getBytes(), (key + "-").getBytes()));  
        for (Result r : rs) {
        	String row = new String(r.getRow());
            System.out.println("获得到rowkey:" + row);  
            for (KeyValue keyValue : r.raw()) {  
            	String num = new String(keyValue.getValue());
                list.add(new KeyValuePair(row, num));
                System.out.println(row + "--" + num);
            }
        }

        System.out.printf("Done Fetching! %d\n", list.size());
        System.out.println("Done Sorting!");
        if (K > list.size()) K = list.size();
        Collections.sort(list);
        for(int i=0; i<K; i++) {
        	KeyValuePair result = list.get(i);
        	System.out.println(result.x + "-----" + result.y);
        }
        Thread.sleep(100000000);
    }
}

