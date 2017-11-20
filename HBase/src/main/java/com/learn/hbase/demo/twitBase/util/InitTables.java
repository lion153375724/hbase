package com.learn.hbase.demo.twitBase.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import com.learn.hbase.demo.twitBase.service.impl.RelationService;
import com.learn.hbase.demo.twitBase.service.impl.TwitService;
import com.learn.hbase.demo.twitBase.service.impl.UserService;

public class InitTables {
	// 声明静态配置
	protected static Connection conn = null;
	protected static Admin admin = null;
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
    public static void initTables(){
    	try {
    		//删除表
    		if (admin.tableExists(TableName.valueOf(UserService.TAB_NAME))) {
        		if(admin.isTableEnabled(TableName.valueOf(UserService.TAB_NAME))){
        			admin.disableTable(TableName.valueOf(UserService.TAB_NAME));
        		}
        		admin.deleteTable(TableName.valueOf(UserService.TAB_NAME));
        	}
    		if (admin.tableExists(TableName.valueOf(TwitService.TABLE_NAME))) {
        		if(admin.isTableEnabled(TableName.valueOf(TwitService.TABLE_NAME))){
        			admin.disableTable(TableName.valueOf(TwitService.TABLE_NAME));
        		}
        		admin.deleteTable(TableName.valueOf(TwitService.TABLE_NAME));
        	}
    		if (admin.tableExists(TableName.valueOf(RelationService.FOLLOWS_TABLE_NAME))) {
        		if(admin.isTableEnabled(TableName.valueOf(RelationService.FOLLOWS_TABLE_NAME))){
        			admin.disableTable(TableName.valueOf(RelationService.FOLLOWS_TABLE_NAME));
        		}
        		admin.deleteTable(TableName.valueOf(RelationService.FOLLOWS_TABLE_NAME));
        	}
    		if (admin.tableExists(TableName.valueOf(RelationService.FOLLOWED_TABLE_NAME))) {
        		if(admin.isTableEnabled(TableName.valueOf(RelationService.FOLLOWED_TABLE_NAME))){
        			admin.disableTable(TableName.valueOf(RelationService.FOLLOWED_TABLE_NAME));
        		}
        		admin.deleteTable(TableName.valueOf(RelationService.FOLLOWED_TABLE_NAME));
        	}
    		
    		//创建表
    		if(admin.tableExists(TableName.valueOf(UserService.TAB_NAME))){
    			System.out.println("User table already exists.");
    		}else{
    			System.out.println("Creating User table...");
    			HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(UserService.TAB_NAME));
  		      	HColumnDescriptor c = new HColumnDescriptor(UserService.TAB_FAMILY);
  		      	desc.addFamily(c);
  		      	admin.createTable(desc);
    			System.out.println("User table created.");
    		}
    		
    		if(admin.tableExists(TableName.valueOf(TwitService.TABLE_NAME))){
    			System.out.println("twits table already exists.");
    		}else{
    			System.out.println("Creating twits table...");
    			HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TwitService.TABLE_NAME));
  		      	HColumnDescriptor c = new HColumnDescriptor(TwitService.TWITS_FAM);
  		      	desc.addFamily(c);
  		      	admin.createTable(desc);
    			System.out.println("User twits created.");
    		}
    		
    		if(admin.tableExists(TableName.valueOf(RelationService.FOLLOWS_TABLE_NAME))){
    			System.out.println("follows table already exists.");
    		}else{
    			System.out.println("Creating follows table...");
    			HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(RelationService.FOLLOWS_TABLE_NAME));
  		      	HColumnDescriptor c = new HColumnDescriptor(RelationService.RELATION_FAM);
  		      	desc.addFamily(c);
  		      	admin.createTable(desc);
    			System.out.println("User follows created.");
    		}
    		
    		if(admin.tableExists(TableName.valueOf(RelationService.FOLLOWED_TABLE_NAME))){
    			System.out.println("followedBy table already exists.");
    		}else{
    			System.out.println("Creating followedBy table...");
    			HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(RelationService.FOLLOWED_TABLE_NAME));
  		      	HColumnDescriptor c = new HColumnDescriptor(RelationService.RELATION_FAM);
  		      	desc.addFamily(c);
  		      	admin.createTable(desc);
    			System.out.println("User followedBy created.");
    		}
    		
    		
		} catch (Exception e) {
			e.printStackTrace();
		}
    	
    }
    
    public static void main(String[] args) {
    	InitTables.initTables();
	}
 
}
