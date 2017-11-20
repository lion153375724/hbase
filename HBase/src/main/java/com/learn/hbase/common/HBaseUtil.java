package com.learn.hbase.common;
/** 
 * 功能描述
 * @author : 
 * @date 创建时间：2017年3月27日 下午5:29:51 
 * @version 1.0  
 */
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseUtil {
    // 声明静态配置
	static Connection conn = null;
	static Admin admin = null;
    static {
    	//默认找hBase-site.xml配置
    	Configuration conf = HBaseConfiguration.create();
    	try {
			conn = ConnectionFactory.createConnection(conf);
			admin = conn.getAdmin();
		} catch (IOException e) {
			System.out.println("获取连接异常！");
			e.printStackTrace();
		}
        //conf.set("hbase.zookeeper.quorum", "localhost");
       // conf.set("hbase.zookeeper.quorum", "10.17.1.234,10.17.1.235,10.17.1.236");
    }

    /**
     * 
     * 功能描述: 创建名字空间
     * @author wwj
     * @date 2017年3月28日 上午10:19:40
     * @parameter namespace 空间名称
     * @return 返回值
     * @throws 异常
     */
    public static void createNameSpace(String nameSpace) throws Exception{
    	//创建名字空间描述符
    	NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(nameSpace);
    	NamespaceDescriptor desc =  builder.build();
    	admin.createNamespace(desc);
    	admin.close();
    }
    
    /**
     * 
     * 功能描述: 删除名字空间
     * @author wwj
     * @date 2017年3月28日 上午10:19:40
     * @parameter namespace 空间名称
     * @return 返回值
     * @throws 异常
     */
    public static void deleteNameSpace(String nameSpace) throws Exception{
    	//创建名字空间描述符
    	admin.deleteNamespace(nameSpace);
    	admin.close();
    }
    /*
     * 创建表
     * 
     * @tableName 表名
     * 
     * @family 列族列表
     */
    public static void creatTable(String tableName, String[] family)
            throws Exception {
    	//创建表描述符
    	HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
    	//创建列族描述符
    	HColumnDescriptor colDesc = null;
    	for(int i=0; i<family.length; i++){
    		colDesc = new HColumnDescriptor(Bytes.toBytes(family[i]));
    		tableDesc.addFamily(colDesc);
    	}
    	
        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("table Exists!");
            System.exit(0);
        } else {
            admin.createTable(tableDesc);
            System.out.println("create table Success!");
        }
        admin.close();
    }
    
    /*
     * 删除表
     * 
     * @tableName 表名
     */
    public static void dropTable(String tableName) throws IOException {
        //先禁用，才能删除
        admin.disableTable(TableName.valueOf("t1"));
        admin.deleteTable(TableName.valueOf("t1"));
        System.out.println(tableName + " is deleted!");
        admin.close();
    }

    /**
     * 
     * 功能描述:put数据
     * @author wwj
     * @date 2017年3月28日 上午10:42:26
     * @parameter tableName 表名
     * 				rowKey
     * 				familyName 列族名称
     * 				column 列族列表
     * 				value 值列表
     * @return 返回值
     * @throws 异常
     */
    public void put(String tableName,String rowKey, String familyName,
            String[] column, String[] value) throws Exception{
    	Table table = conn.getTable(TableName.valueOf(tableName));
    	//设置rowKey
    	Put put = new Put(Bytes.toBytes(rowKey));
    	// 获取所有的列族
		HColumnDescriptor[] columnFamilies = table.getTableDescriptor() .getColumnFamilies();
		String fName = "";
    	for(int i=0; i< columnFamilies.length; i++){
    		fName = columnFamilies[i].getNameAsString(); //攻取列族名称
    		if(fName.equals(familyName)){
    			for(int j=0; j<column.length; j++ ){
    				put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column[j]), Bytes.toBytes(value[j]));
    			}
    		}
    	}
    	
    	table.put(put);
    	System.out.println("put success!");
    	table.close();
    
    }
    
    /**
     * 
     * 功能描述: 在进行大批量的数据更新时，可使用table.setAutoFlushTo(false),关闭自动缓冲，table.flushCommits()来提交，可大大提高效率
     * @author wwj
     * @date 2017年3月29日 下午4:18:19
     * @parameter 参数
     * @return 返回值
     * @throws 异常
     */
    public void clientBuffer(String tableName) throws Exception{
    	HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
    	table.setAutoFlushTo(false);
    	Put put = null;
    	long start = System.currentTimeMillis();
    	// 获取所有的列族
    	for(int i=3; i< 10000; i++){
    		put = new Put(Bytes.toBytes("row"+i));
    		put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes("tom"));
    		table.put(put);
    	}
    	table.flushCommits();
    	System.out.println("put success!");
    	table.close();
    	System.out.println(System.currentTimeMillis() - start);
    }
    
    /**
     * 
     * 功能描述: checkAndPut检查并更新
     * @author wwj
     * @date 2017年3月29日 下午4:18:19
     * @parameter 参数
     * @return 返回值
     * @throws 异常
     */
    public void checkAndPut(String tableName) throws Exception{
    	HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
    	table.setAutoFlushTo(false);
    	Put put = new Put(Bytes.toBytes("row1"));
    	put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes("tom"));
    	//如果对应值为空则进行上面定义的put
    	table.checkAndPut(Bytes.toBytes("row1"), Bytes.toBytes("cf1"), Bytes.toBytes("name"), null, put);
    	System.out.println("put success!");
    	table.close();
    }
    
    /**
     * 
     * 功能描述: 检查数据是否存在
     * @author wwj
     * @date 2017年3月29日 下午4:18:19
     * @parameter 参数
     * @return 返回值
     * @throws 异常
     */
    public boolean exists(String tableName) throws Exception{
    	HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
    	Get get = new Get(Bytes.toBytes("row1"));
    	get.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
    	table.close();
    	return table.exists(get);
    }
    
    
    /** 删除指定的列
    * 
    * @tableName 表名
    * 
    * @rowKey rowKey
    * 
    * @familyName 列族名
    * 
    * @columnName 列名
    * 
    */
    
   public static void deleteColumn(String tableName, String rowKey,
           String falilyName, String columnName) throws IOException {
       Table table = conn.getTable(TableName.valueOf(tableName));
       Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
       deleteColumn.addColumn(Bytes.toBytes(falilyName),
               Bytes.toBytes(columnName));
       table.delete(deleteColumn);
       System.out.println(falilyName + ":" + columnName + "is deleted!");
   }
    
    /**
     * 
     * 功能描述:根据rowkey查询
     * @author wwj
     * @date 2017年3月28日 上午11:14:25
     * @parameter 参数 tableName表名  rowKey列
     * @return 返回值 
     * @throws 异常
     */
    public Result listByRowKey(String tableName,String rowkey) throws Exception{
    	Table t = conn.getTable(TableName.valueOf(tableName));
    	Get get = new Get(Bytes.toBytes(rowkey));
    	Result result = t.get(get);
    	System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("no"))));
    	System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("name"))));
    	System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("age"))));
    	System.out.println("---------------------------------------------------------");
    	/*List<Cell> cellList = result.listCells();
    	for (Cell cell : cellList) {
            System.out.println("family:" + Bytes.toString(cell..getFamilyArray()));
            System.out
                    .println("qualifier:" + Bytes.toString(cell.getQualifierArray()));
            System.out.println("value:" + Bytes.toString(cell.getValueArray()));
            System.out.println("Timestamp:" + cell.getTimestamp());
        }*/
    	t.close();
    	return result;
    }
    
    //带条件查询：版本versions查询
    public Result get(String tableName,String rowkey) throws Exception{
    	Table t = conn.getTable(TableName.valueOf(tableName));
    	Get get = new Get(Bytes.toBytes(rowkey));
    	get.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("name"));
    	get.setMaxVersions(3);
    	//get.setTimeRange(minStamp, maxStamp)
    	Result result = t.get(get);
    	List<Cell> cellList = result.listCells();
    	for (Cell cell : cellList) {
            System.out.println("family:" + Bytes.toString(cell.getFamily()));
            System.out
                    .println("qualifier:" + Bytes.toString(cell.getQualifier()));
            System.out.println("value:" + Bytes.toString(cell.getValue()));
            System.out.println("Timestamp:" + cell.getTimestamp());
            
        }
    	t.close();
    	return result;
    }
    
    
    /**
     * 
     * 功能描述:scan 用法
     * @author wwj
     * @date 2017年3月28日 上午11:22:40
     * @parameter 参数
     * @return 返回值
     * @throws 异常
     */
    public ResultScanner listAll(String tableName) throws Exception{
    	//多少行到多少行
    	//Scan scan = new Scan(Bytes.toBytes("row1"),Bytes.toBytes("row10"));
    	Scan scan = new Scan();
    	Table table = conn.getTable(TableName.valueOf(tableName));
    	ResultScanner rs = null;
    	rs = table.getScanner(scan);
    	
    	Iterator<Result> it = rs.iterator();
    	while(it.hasNext()){
    		Result result = it.next();
    		
    		System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("orderNo"))));
        	System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("amount"))));
        	System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("createTime"))));
	       
    	}
    	table.close();
    	rs.close();
    	return rs;
    }
    
    
    public void scanningCache() throws Exception{
    	Table table = conn.getTable(TableName.valueOf("ns1:t1"));
    	long start = System.currentTimeMillis();
    	Scan scan = new Scan();
    	scan.setCaching(100);
    	System.out.println(scan.getCaching());
    	scan.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
    	ResultScanner scanner = table.getScanner(scan);
    	Iterator<Result> it = scanner.iterator();
    	Result result = null;
    	while(it.hasNext()){
    		result = it.next();
    		Bytes.toString(result.getColumnLatestCell(Bytes.toBytes("cf1"), Bytes.toBytes("name")).getValue());
    	}
    	System.out.println(System.currentTimeMillis()-start);
    }
    
    
    public void batch() throws Exception{
    	Table table = conn.getTable(TableName.valueOf("ns1:t1"));
    	long start = System.currentTimeMillis();
    	Scan scan = new Scan();
    	scan.setBatch(2); //依实际查询的记录设置，一次查询返回的列，默认为1
    	System.out.println(scan.getCaching());
    	scan.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
    	ResultScanner scanner = table.getScanner(scan);
    	Iterator<Result> it = scanner.iterator();
    	Result result = null;
    	while(it.hasNext()){
    		result = it.next();
    		Bytes.toString(result.getColumnLatestCell(Bytes.toBytes("cf1"), Bytes.toBytes("name")).getValue());
    	}
    	System.out.println(System.currentTimeMillis()-start);
    }
    
    /**
     * 复杂过滤器
     * 功能描述:
     * @author wwj
     * @date 2017年3月29日 下午5:33:08
     * @parameter 参数
     * @return 返回值
     * @throws 异常
     */
    public void complexFilter()throws Exception{
    	Table table = conn.getTable(TableName.valueOf("ns1:t1"));
    	Scan scan = new Scan();
    	byte[] f = Bytes.toBytes("cf1");
    	byte[] name= Bytes.toBytes("name");
    	byte[] age = Bytes.toBytes("age");
    	
    	//where ((name like 't%') and (age > 20)) or ((name like '%t') and (age<20))
    	
    	//name like 't%'
    	SingleColumnValueFilter ft1 = new SingleColumnValueFilter(f, name, CompareOp.EQUAL, new RegexStringComparator("^t"));
    	//age > 20
    	SingleColumnValueFilter ft2 = new SingleColumnValueFilter(f, age, CompareOp.GREATER, new BinaryComparator(Bytes.toBytes(20)));
    	
    	FilterList fTop = new FilterList(Operator.MUST_PASS_ALL,ft1,ft2);
    	
    	//name like 't%'
    	SingleColumnValueFilter fb1 = new SingleColumnValueFilter(f, name, CompareOp.EQUAL, new RegexStringComparator("t$"));
    	//age > 20
    	SingleColumnValueFilter fb2 = new SingleColumnValueFilter(f, age, CompareOp.GREATER, new BinaryComparator(Bytes.toBytes(20)));
    	
    	//fb1 and fb2
    	FilterList fBootom = new FilterList(Operator.MUST_PASS_ALL,fb1,fb2);
    	
    	//ftop or fbottom
    	FilterList fall = new FilterList(Operator.MUST_PASS_ONE,fTop,fBootom);
    	
    	scan.setFilter(fall);
    	ResultScanner scanner = table.getScanner(scan);
    	Iterator<Result> it = scanner.iterator();
    	Result result = null;
    	while(it.hasNext()){
    		result = it.next();
    		//System.out.println(result);
    		Map<byte[],byte[]> map = result.getFamilyMap(Bytes.toBytes("cf1"));
    		for(Entry<byte[],byte[]> entry : map.entrySet()){
    			System.out.println(Bytes.toString(entry.getKey()) + ":" + Bytes.toString(entry.getValue()));
    		}
    	}
    }
    
    //分页过滤器
    public void pageFilter() throws Exception{
    	Table table = conn.getTable(TableName.valueOf("ns1:t1"));
    	Scan scan = new Scan();
    	PageFilter filter = new PageFilter(10);
    	scan.setFilter(filter);
    	outputResult(table.getScanner(scan));
    }
    
    //PrefixFilter :判断rowkey的过滤器
    public void prefixFilter() throws Exception{
    	Table table = conn.getTable(TableName.valueOf("ns1:t1"));
    	Scan scan = new Scan();
    	PrefixFilter filter = new PrefixFilter(Bytes.toBytes("row88"));
    	scan.setFilter(filter);
    	outputResult(table.getScanner(scan));
    }
    
    private void outputResult(ResultScanner scanner) throws Exception{
    	Iterator<Result> it = scanner.iterator();
    	Result result = null;
    	while(it.hasNext()){
    		result = it.next();
    		System.out.println(result);
    		Map<byte[],byte[]> map = result.getFamilyMap(Bytes.toBytes("cf1"));
    		for(Entry<byte[],byte[]> entry : map.entrySet()){
    			System.out.println(Bytes.toString(entry.getKey()) + ":" + Bytes.toString(entry.getValue()));
    		}
    	}
    }
   /* 
     * 为表添加数据（适合知道有多少列族的固定表）
     * 
     * @rowKey rowKey
     * 
     * @tableName 表名
     * 
     * @column1 第一个列族列表
     * 
     * @value1 第一个列的值的列表
     * 
     * @column2 第二个列族列表
     * 
     * @value2 第二个列的值的列表
     
    public static void addData(String rowKey, String tableName,
            String[] column1, String[] value1, String[] column2, String[] value2)
            throws IOException {
        Put put = new Put(Bytes.toBytes(rowKey));// 设置rowkey
        HTable table = new HTable(conf, Bytes.toBytes(tableName));// HTabel负责跟记录相关的操作如增删改查等//
                                                                    // 获取表
        HColumnDescriptor[] columnFamilies = table.getTableDescriptor() // 获取所有的列族
                .getColumnFamilies();

        for (int i = 0; i < columnFamilies.length; i++) {
            String familyName = columnFamilies[i].getNameAsString(); // 获取列族名
            if (familyName.equals("article")) { // article列族put数据
                for (int j = 0; j < column1.length; j++) {
                    put.add(Bytes.toBytes(familyName),
                            Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
                }
            }
            if (familyName.equals("author")) { // author列族put数据
                for (int j = 0; j < column2.length; j++) {
                    put.add(Bytes.toBytes(familyName),
                            Bytes.toBytes(column2[j]), Bytes.toBytes(value2[j]));
                }
            }
        }
        table.put(put);
        System.out.println("add data Success!");
    }

    
     * 根据rwokey查询
     * 
     * @rowKey rowKey
     * 
     * @tableName 表名
     
    public static Result getResult(String tableName, String rowKey)
            throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        HTable table = new HTable(conf, Bytes.toBytes(tableName));// 获取表
        Result result = table.get(get);
        for (KeyValue kv : result.list()) {
            System.out.println("family:" + Bytes.toString(kv.getFamily()));
            System.out
                    .println("qualifier:" + Bytes.toString(kv.getQualifier()));
            System.out.println("value:" + Bytes.toString(kv.getValue()));
            System.out.println("Timestamp:" + kv.getTimestamp());
            System.out.println("-------------------------------------------");
        }
        return result;
    }

    
     * 遍历查询hbase表
     * 
     * @tableName 表名
     
    public static void getResultScann(String tableName) throws IOException {
        Scan scan = new Scan();
        ResultScanner rs = null;
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        try {
            rs = table.getScanner(scan);
            for (Result r : rs) {
                for (KeyValue kv : r.list()) {
                    System.out.println("row:" + Bytes.toString(kv.getRow()));
                    System.out.println("family:"
                            + Bytes.toString(kv.getFamily()));
                    System.out.println("qualifier:"
                            + Bytes.toString(kv.getQualifier()));
                    System.out
                            .println("value:" + Bytes.toString(kv.getValue()));
                    System.out.println("timestamp:" + kv.getTimestamp());
                    System.out
                            .println("-------------------------------------------");
                }
            }
        } finally {
            rs.close();
        }
    }

    
     * 遍历查询hbase表
     * 
     * @tableName 表名
     
    public static void getResultScann(String tableName, String start_rowkey,
            String stop_rowkey) throws IOException {
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(start_rowkey));
        scan.setStopRow(Bytes.toBytes(stop_rowkey));
        ResultScanner rs = null;
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        try {
            rs = table.getScanner(scan);
            for (Result r : rs) {
                for (KeyValue kv : r.list()) {
                    System.out.println("row:" + Bytes.toString(kv.getRow()));
                    System.out.println("family:"
                            + Bytes.toString(kv.getFamily()));
                    System.out.println("qualifier:"
                            + Bytes.toString(kv.getQualifier()));
                    System.out
                            .println("value:" + Bytes.toString(kv.getValue()));
                    System.out.println("timestamp:" + kv.getTimestamp());
                    System.out
                            .println("-------------------------------------------");
                }
            }
        } finally {
            rs.close();
        }
    }

    
     * 查询表中的某一列
     * 
     * @tableName 表名
     * 
     * @rowKey rowKey
     
    public static void getResultByColumn(String tableName, String rowKey,
            String familyName, String columnName) throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName)); // 获取指定列族和列修饰符对应的列
        Result result = table.get(get);
        for (KeyValue kv : result.list()) {
            System.out.println("family:" + Bytes.toString(kv.getFamily()));
            System.out
                    .println("qualifier:" + Bytes.toString(kv.getQualifier()));
            System.out.println("value:" + Bytes.toString(kv.getValue()));
            System.out.println("Timestamp:" + kv.getTimestamp());
            System.out.println("-------------------------------------------");
        }
    }

    
     * 更新表中的某一列
     * 
     * @tableName 表名
     * 
     * @rowKey rowKey
     * 
     * @familyName 列族名
     * 
     * @columnName 列名
     * 
     * @value 更新后的值
     
    public static void updateTable(String tableName, String rowKey,
            String familyName, String columnName, String value)
            throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName),
                Bytes.toBytes(value));
        table.put(put);
        System.out.println("update table Success!");
    }

    
     * 查询某列数据的多个版本
     * 
     * @tableName 表名
     * 
     * @rowKey rowKey
     * 
     * @familyName 列族名
     * 
     * @columnName 列名
     
    public static void getResultByVersion(String tableName, String rowKey,
            String familyName, String columnName) throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
        get.setMaxVersions(5);
        Result result = table.get(get);
        for (KeyValue kv : result.list()) {
            System.out.println("family:" + Bytes.toString(kv.getFamily()));
            System.out
                    .println("qualifier:" + Bytes.toString(kv.getQualifier()));
            System.out.println("value:" + Bytes.toString(kv.getValue()));
            System.out.println("Timestamp:" + kv.getTimestamp());
            System.out.println("-------------------------------------------");
        }
        
         * List<?> results = table.get(get).list(); Iterator<?> it =
         * results.iterator(); while (it.hasNext()) {
         * System.out.println(it.next().toString()); }
         
    }

    
     * 删除指定的列
     * 
     * @tableName 表名
     * 
     * @rowKey rowKey
     * 
     * @familyName 列族名
     * 
     * @columnName 列名
     
    public static void deleteColumn(String tableName, String rowKey,
            String falilyName, String columnName) throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
        deleteColumn.deleteColumns(Bytes.toBytes(falilyName),
                Bytes.toBytes(columnName));
        table.delete(deleteColumn);
        System.out.println(falilyName + ":" + columnName + "is deleted!");
    }

    
     * 删除指定的列
     * 
     * @tableName 表名
     * 
     * @rowKey rowKey
     
    public static void deleteAllColumn(String tableName, String rowKey)
            throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Delete deleteAll = new Delete(Bytes.toBytes(rowKey));
        table.delete(deleteAll);
        System.out.println("all columns are deleted!");
    }

   

    public static void main(String[] args) throws Exception {

        // 创建表
        String tableName = "blog2";
        String[] family = { "article", "author" };
        // creatTable(tableName, family);

        // 为表添加数据

        String[] column1 = { "title", "content", "tag" };
        String[] value1 = {
                "Head First HBase",
                "HBase is the Hadoop database. Use it when you need random, realtime read/write access to your Big Data.",
                "Hadoop,HBase,NoSQL" };
        String[] column2 = { "name", "nickname" };
        String[] value2 = { "nicholas", "lee" };
        addData("rowkey1", "blog2", column1, value1, column2, value2);
        addData("rowkey2", "blog2", column1, value1, column2, value2);
        addData("rowkey3", "blog2", column1, value1, column2, value2);

        // 遍历查询
        getResultScann("blog2", "rowkey4", "rowkey5");
        // 根据row key范围遍历查询
        getResultScann("blog2", "rowkey4", "rowkey5");

        // 查询
        getResult("blog2", "rowkey1");

        // 查询某一列的值
        getResultByColumn("blog2", "rowkey1", "author", "name");

        // 更新列
        updateTable("blog2", "rowkey1", "author", "name", "bin");

        // 查询某一列的值
        getResultByColumn("blog2", "rowkey1", "author", "name");

        // 查询某列的多版本
        getResultByVersion("blog2", "rowkey1", "author", "name");

        // 删除一列
        deleteColumn("blog2", "rowkey1", "author", "nickname");

        // 删除所有列
        deleteAllColumn("blog2", "rowkey1");

        // 删除表
        deleteTable("blog2");

    }*/
}
