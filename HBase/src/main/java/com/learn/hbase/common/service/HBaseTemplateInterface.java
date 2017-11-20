package com.learn.hbase.common.service;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.filter.FilterList;

import com.learn.hbase.common.PageModel;
import com.learn.hbase.demo.twitBase.model.User;

public interface HBaseTemplateInterface {

	/**
	 * 根据rowkeys获取返回值,返回值为List<Object>
	 * 传递的参数会需要返回值的类型.同时Object必须包含@ORMHBaseColumn 与@ORMHBaseColumnTable
	 * @param <T>
	 * 
	 * @param rowKey
	 * @return
	 * @throws IOException
	 */
	public <T> List<T> get(T obj, String... rowkeys);
	
	/**
	 * 根据表名称删除指定的rowkey,rowkey的值为HBase的主键
	 * 
	 * @param rowkey
	 */
	public void delete(String tableName, String... rowkeys);

	/**
	 * 插入Object数据到相关库中.Object中的Id必须为Rowkey,同时必须定义id的HBaseColumn的两个值为Rowkey.类似于:
	 * @param <T>
	 * 
	 * @ORMHBaseColumn(family="rowkey",qualifier="rowkey")
	 * private String id;
	 * 
	 * 其他字段的family,qualifier值与HBase库中的一致.如果库中没有family需要手动创建family.qualifier则可以任意插入.
	 * 
	 * @param objs
	 */
	public <T> void insert(T... objs);
	
	/**
	 * 给予表名称
	 * 插入Object...	
	 * 数据到相关的表中,Object中声明的
	 * @param <T>
	 * @ORMHbaseTable 中定义的HBase表名称无效. Object中的Id必须为Rowkey,同时必须定义id的HBaseColumn的两个值为Rowkey.类似于:
	 * @ORMHBaseColumn(family="rowkey",qualifier="rowkey")
	 * private String id;
	 * 
	 * 其他字段的family,qualifier值与HBase库中的一致.如果库中没有family需要手动创建family.qualifier则可以任意插入.
	 * 
	 * 
	 * 
	 * @param tableName
	 * @param objs
	 */
	public <T> void insert(String tableName,T...objs);

	/**
	 * 根据Object获取到 @ORMHBaseTable 中的tableName.然后基于rowkeys删除数据.如果不需要定义Object则可以直接调用
	 * delete(String tableName,String...rowkeys)
	 * @param <T>
	 * 
	 * @param rowkey
	 */
	public <T> void delete(T obj, String... rowkeys);
	
	/**
	 * 获取到当前链接内的所有Table名称
	 * @return List<tableName>
	 */
	public List<String> tables();
	
	/**
	 * table是否存在
	 * @param tableName
	 * @return
	 */
	public boolean existTable(String tableName);
	/**
	 * 获取到表中的所有familys<列簇>名称.
	 * @param tableName
	 * @return
	 */
	public List<String> familys(String tableName);
	
	/**
	 * 传递表名称与familys列簇创建表.
	 * 如果表名已经存在,那么不会创建表.直接打印日志.
	 * @param tableName
	 * @param columnFamilys
	 */
	public void createTable(String tableName,String... columnFamilys);
	
	/**
	 * 删除表.当表存在时,会直接删除该表数据
	 * @param tableName
	 */
	public void deleteTable(String tableName);
	
	/**
	 * 为已经存在的表添加列簇.列簇可为一个或者多个.当表不存在时,不进行操作.
	 * 
	 * @param tableName
	 * @param columnFamilys
	 */
	public void addColumnFamilys(String tableName,String... columnFamilys);
	
	/**
	 * 删除表中的列簇.
	 * @param tableName
	 * @param columnFamilys
	 */
	public void deleteColumnFamilys(String tableName,String...columnFamilys);
	
}
