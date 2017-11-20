package com.example.demo;

import junit.framework.TestCase;

import com.learn.hbase.common.HBaseUtil;

/** 
 * 功能描述
 * @author : 
 * @date 创建时间：2017年3月27日 下午6:21:25 
 * @version 1.0  
 */
public class HBaseTest extends TestCase{

	public void testHbase() throws Exception{
		/*//创建配置对象
		Configuration conf = HBaseConfiguration.create();
		//通过连接工厂创建连接对象,默认找hBase-site.xml配置
		//conf.set("hbase.zookeeper.quorum", "10.17.1.234,10.17.1.235,10.17.1.236");
		Connection conn = ConnectionFactory.createConnection(conf);
		//通过连接对象获取table信息
		Table table = conn.getTable(TableName.valueOf("t1"));
		//设定row no
		Put put = new Put(Bytes.toBytes("row1"));
		//设置字段值
		put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("zy"), Bytes.toBytes("dev"));
		
		//put插入表
		table.put(put);
		table.close();
		conn.close();*/
		
		/*HBaseUtil util = new HBaseUtil();
		util.getResultScann("t1");*/
	}
	
	public void testCreateNameSpace() throws Exception{
		HBaseUtil hbUtil = new HBaseUtil();
		hbUtil.createNameSpace("ns1");
	}
	
	public void testCreateTable() throws Exception{
		HBaseUtil hbUtil = new HBaseUtil();
		hbUtil.creatTable("ns1:t1", new String[]{"cf1","cf2"});
	}
	
	public void testDropTable() throws Exception{
		HBaseUtil hbUtil = new HBaseUtil();
		hbUtil.dropTable("t1");
	}
	
	public void testput() throws Exception{
		HBaseUtil hbUtil = new HBaseUtil();
		hbUtil.put("ns1:t1", "row1", "cf1", new String[]{"no","name","age",}, new String[]{"no003","Tom55","20"});
		hbUtil.put("ns1:t1", "row2", "cf1", new String[]{"no","name","age"}, new String[]{"no004","sack66","30"});
	}
	
	public void testListByRowKey() throws Exception{
		HBaseUtil hbUtil = new HBaseUtil();
		System.out.println(hbUtil.listByRowKey("ns1:t1", "row9999"));
	}
	
	public void exists() throws Exception{
		HBaseUtil hbUtil = new HBaseUtil();
		System.out.println(hbUtil.exists("ns1:t1"));
	}
	
	public void scanningCache() throws Exception{
		HBaseUtil hbUtil = new HBaseUtil();
		hbUtil.scanningCache();
	}
	
	public void batch() throws Exception{
		HBaseUtil hbUtil = new HBaseUtil();
		hbUtil.batch();
	}
	
	public void complexFilter() throws Exception{
		HBaseUtil hbUtil = new HBaseUtil();
		hbUtil.complexFilter();
	}
	
	public void testpageFilter() throws Exception{
		HBaseUtil hbUtil = new HBaseUtil();
		hbUtil.pageFilter();
	}
	
	public void prefixFilter() throws Exception{
		HBaseUtil hbUtil = new HBaseUtil();
		hbUtil.prefixFilter();
	}
	
	
	public void testget() throws Exception{
		HBaseUtil hbUtil = new HBaseUtil();
		System.out.println(hbUtil.get("ns1:t1", "row1"));
	}
	
	public void testclientBuffer() throws Exception{
		HBaseUtil hbUtil = new HBaseUtil();
		hbUtil.clientBuffer("ns1:t1");
	}
	
	
	public void testListAll() throws Exception{
		HBaseUtil hbUtil = new HBaseUtil();
		System.out.println(hbUtil.listAll("ns1:order"));
	}
	
	public void Testdelete() throws Exception{
		HBaseUtil hbUtil = new HBaseUtil();
		hbUtil.deleteColumn("ns1:t1", "row1", "cf2", "age");
	}
	
}
