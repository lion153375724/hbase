package com.learn.hbase.common.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
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
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.learn.hbase.common.HBasePageModel;
import com.learn.hbase.common.PageModel;
import com.learn.hbase.common.service.HBaseTemplateInterface;
import com.learn.hbase.demo.twitBase.model.User;
import com.learn.hbase.util.HBaseUtil;
import com.learn.hbase.util.ORMHBaseTable;
/**
 * 
 * @author jason
 * @createTime 2017年11月7日下午2:55:01
 */
@Service
public class AbstracterHBaseTemplate implements HBaseTemplateInterface {
	private Logger logger = LoggerFactory
			.getLogger(AbstracterHBaseTemplate.class);
	//private Configuration config;
	
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
    
    /**
	 * 获取配置，当前使用默认配置
	 * 
	 * @return the config
	 */
	/*public Configuration getConfig() {
		if (config == null) {
			config = HBaseConfiguration.create();
		}
		return config;
	}*/

	/**
	 * 
	 */
	public AbstracterHBaseTemplate() {
	}

	

	public <T> List<T> get(T obj, String... rowkeys) {
		List<T> objs = new ArrayList<T>();
		String tableName = getORMTable(obj);
		if (StringUtils.isBlank(tableName)) {
			return objs;
		}
		List<Result> results = getResults(tableName, rowkeys);
		if (results.isEmpty()) {
			return objs;
		}
		for (int i = 0; i < results.size(); i++) {
			T bean = null;
			Result result = results.get(i);
			if (result == null || result.isEmpty()) {
				continue;
			}
			try {
				bean = HBaseUtil.result2Bean(result, obj);
				objs.add(bean);
			} catch (Exception e) {
				logger.warn("", e);
			}
		}
		return objs;
	}
	
	public void delete(String tableName, String... rowkeys) {
		List<Delete> deletes = new ArrayList<Delete>();
		for (String rowkey : rowkeys) {
			if (StringUtils.isBlank(rowkey)) {
				continue;
			}
			deletes.add(new Delete(Bytes.toBytes(rowkey)));
		}
		delete(deletes, tableName);
	}

	public <T> void delete(T obj, String... rowkeys) {
		String tableName = "";
		tableName = getORMTable(obj);
		if (StringUtils.isBlank(tableName)) {
			return;
		}
		List<Delete> deletes = new ArrayList<Delete>();
		for (String rowkey : rowkeys) {
			if (StringUtils.isBlank(rowkey)) {
				continue;
			}
			deletes.add(new Delete(Bytes.toBytes(rowkey)));
		}
		delete(deletes, tableName);
	}
	
	public <T> void insert(T... objs) {
		List<Put> puts = new ArrayList<Put>();
		String tableName = "";
		for (Object obj : objs) {
			if (obj == null) {
				continue;
			}
			tableName = getORMTable(obj);
			try {
				Put put = HBaseUtil.bean2Put(obj);
				puts.add(put);
			} catch (Exception e) {
				logger.warn("", e);
			}
		}
		savePut(puts, tableName);
	}
	
	public <T> void insert(String tableName, T... objs) {
		List<Put> puts = new ArrayList<Put>();
		for (Object obj : objs) {
			if (obj == null) {
				continue;
			}
			try {
				Put put = HBaseUtil.bean2Put(obj);
				puts.add(put);
			} catch (Exception e) {
				logger.warn("", e);
			}
		}
		savePut(puts, tableName);
	}

	private void delete(List<Delete> deletes, String tableName) {
		Table table = null;
		try {
			if (StringUtils.isBlank(tableName)) {
				return;
			}
			table = conn.getTable(TableName.valueOf(tableName));
			table.delete(deletes);
		} catch (IOException e) {
			logger.warn("执行删除失败;",e);
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					logger.warn("", e);
				}
			}
		}
	}
	
	public List<String> familys(String tableName) {
		Table table = null;
		try {
			List<String> columns = new ArrayList<String>();
			table = conn.getTable(TableName.valueOf(tableName));
			if (table==null) {
				return columns;
			}
			HTableDescriptor tableDescriptor= table.getTableDescriptor();
			HColumnDescriptor[] columnDescriptors = tableDescriptor.getColumnFamilies();
			for (HColumnDescriptor columnDescriptor :columnDescriptors) {
				String columnName = columnDescriptor.getNameAsString();
				columns.add(columnName);
			}
			return columns;
		} catch (Exception e) {
			logger.warn("",e);
		}finally{
			if (table!=null) {
				try {
					table.close();
				} catch (IOException e) {
					logger.warn("",e);
				}
			}
		}
		return new ArrayList<String>();
	}

	public List<String> tables() {
		try {
			TableName[] tableNames = admin.listTableNames();
			List<String> tables = new ArrayList<String>();
			for (TableName tableName : tableNames) {
				String name = tableName.getNameAsString();
				tables.add(name);
			}
			return tables;
		} catch (Exception e) {
			logger.warn("", e);
		} finally {
			if (admin != null) {
				try {
					admin.close();
				} catch (IOException e) {
					logger.warn("", e);
				}
			}
		}
		return new ArrayList<String>();
	}

	public boolean existTable(String tableName){
		try {
			return admin.tableExists(TableName.valueOf(tableName));
		} catch (Exception e) {
			// TODO: handle exception
		}
		return false;
	}
	
	public void createTable(String tableName, String... columnFamilys) {
		try {
			HTable table = (HTable) conn.getTable(TableName.valueOf(tableName));
	    	Get get = new Get(Bytes.toBytes("row1"));
	    	get.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
	    	table.close();
			if (table.exists(get)) {
				logger.warn("HBase中已经存在该命名的表,请修改名称或删除该表后重建");
				return;
			}
			HTableDescriptor tableDescriptor = new HTableDescriptor(
					TableName.valueOf(tableName));
			for (String columnFamily : columnFamilys) {
				tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
			}
			admin.createTable(tableDescriptor);
		} catch (Exception e) {
			logger.warn("创建HBase表失败;",e);
		} finally {
			if (admin != null) {
				try {
					admin.close();
				} catch (IOException e) {
					logger.warn("", e);
				}
			}
		}
	}

	public void deleteTable(String tableName) {
		try {
			admin.disableTable(TableName.valueOf(tableName));
			admin.deleteTable(TableName.valueOf(tableName));
		} catch (Exception e) {
			logger.warn("", e);
		} finally {
			if (admin != null) {
				try {
					admin.close();
				} catch (IOException e) {
					logger.warn("", e);
				}
			}
		}
	}

	public void addColumnFamilys(String tableName, String... columnFamilys) {
		try {
			if (!admin.tableExists(TableName.valueOf(tableName))) {
				return;
			}
			for (String family : columnFamilys) {
				admin.addColumn(TableName.valueOf(tableName), new HColumnDescriptor(Bytes.toBytes(family)));
			}
			admin.flush(TableName.valueOf(tableName));
		} catch (Exception e) {
			logger.warn("", e);
		}finally{
			if (admin!=null) {
				try {
					admin.close();
				} catch (IOException e) {
					logger.warn("",e);
				}
			}
		}

	}
	
	public void deleteColumnFamilys(String tableName, String... columnFamilys) {
		try {
			if (!admin.tableExists(TableName.valueOf(tableName))) {
				return;
			}
			for (String family : columnFamilys) {
				admin.deleteColumn(TableName.valueOf(tableName), Bytes.toBytes(family));
			}
			admin.flush(TableName.valueOf(tableName));
		} catch (Exception e) {
			logger.warn("", e);
		}finally{
			if (admin!=null) {
				try {
					admin.close();
				} catch (IOException e) {
					logger.warn("",e);
				}
			}
		}
	}

	private void savePut(List<Put> puts, String tableName) {
		Table table = null;
		try {
			if (StringUtils.isBlank(tableName)) {
				return;
			}
			table = conn.getTable(TableName.valueOf(tableName));
			table.put(puts);
		} catch (IOException e) {
			logger.warn("存储失败;",e);
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					logger.warn("", e);
				}
			}
		}
	}

	private List<Result> getResults(String tableName, String... rowkeys) {
		List<Result> resultList = new ArrayList<Result>();
		List<Get> gets = new ArrayList<Get>();
		for (String rowkey : rowkeys) {
			if (StringUtils.isBlank(rowkey)) {
				continue;
			}
			Get get = new Get(Bytes.toBytes(rowkey));
			gets.add(get);
		}
		Table table = null;
		
		try {
			table = conn.getTable(TableName.valueOf(tableName));
			Result[] results = table.get(gets);
			Collections.addAll(resultList, results);
			return resultList;
		} catch (IOException e) {
			logger.warn("", e);
			return resultList;
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					logger.warn("", e);
				}
			}
			
		}

	}
	
	public String getORMTable(Object obj) {
		ORMHBaseTable table = obj.getClass().getAnnotation(ORMHBaseTable.class);
		return table.tableName();
	}
	
	


	 // 获取扫描器对象
    private static Scan getScan(String startRow, String stopRow) {
        Scan scan = new Scan();
        scan.setStartRow(getBytes(startRow));
        scan.setStopRow(getBytes(stopRow));
        return scan;
    }

     //转换byte数组 
    public static byte[] getBytes(String str) {
        if (str == null)
            str = "";
        return Bytes.toBytes(str);
    }
    
	  /**
     * 查询数据
     * @param tableKey 表标识
     * @param queryKey 查询标识
     * @param startRow 开始行
     * @param paramsMap 参数集合
     * @return 结果集
     */
    public static HBasePageModel getDataMap(String tableName, String startRow,
            String stopRow, Integer currentPage, Integer pageSize)
            throws IOException {
        List<Map<String, String>> mapList = new LinkedList<Map<String, String>>();
        ResultScanner scanner = null;
        // 为分页创建的封装类对象，下面有给出具体属性
        HBasePageModel tbData = null;
        try {
            // 获取最大返回结果数量
            if (pageSize == null || pageSize == 0L)
                pageSize = 100;
            if (currentPage == null || currentPage == 0)
                currentPage = 1;
            // 计算起始页和结束页
            Integer firstPage = (currentPage - 1) * pageSize;
            Integer endPage = firstPage + pageSize;
            // 从表池中取出HBASE表对象
            Table table = conn.getTable(TableName.valueOf(tableName));
            // 获取筛选对象
            Scan scan = getScan(startRow, stopRow);
            // 给筛选对象放入过滤器(true标识分页,具体方法在下面)
            scan.setFilter(packageFilters(true));
            // 缓存1000条数据
            scan.setCaching(1000);
            scan.setCacheBlocks(false);
            scanner = table.getScanner(scan);
            int i = 0;
            List<byte[]> rowList = new LinkedList<byte[]>();
            // 遍历扫描器对象， 并将需要查询出来的数据row key取出
            for (Result result : scanner) {
                String row = toStr(result.getRow());
                if (i >= firstPage && i < endPage) {
                    rowList.add(getBytes(row));
                }
                i++;
            }
            // 获取取出的row key的GET对象
            List<Get> getList = getList(rowList);
            Result[] results = table.get(getList);
            // 遍历结果
            for (Result result : results) {
                Map<byte[], byte[]> fmap = packFamilyMap(result);
                Map<String, String> rmap = packRowMap(fmap);
                mapList.add(rmap);
            }
            // 封装分页对象
            tbData = new HBasePageModel();
            tbData.setCurrentPage(currentPage);
            tbData.setPageSize(pageSize);
            tbData.setTotalCount(i);
            tbData.setTotalPage(getTotalPage(pageSize, i));
            tbData.setResultList(mapList);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            closeScanner(scanner);
        }
        return tbData;
    }
    private static int getTotalPage(int pageSize, int totalCount) {
        int n = totalCount / pageSize;
        if (totalCount % pageSize == 0) {
            return n;
        } else {
            return ((int) n) + 1;
        }
    }
    
   /**
    * 封装查询条件
    * @param isPage
    * @return
    */
    private static FilterList packageFilters(boolean isPage) {
        FilterList filterList = null;
        // MUST_PASS_ALL(条件 AND) MUST_PASS_ONE（条件OR）
        filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        Filter filter1 = null;
        Filter filter2 = null;
        filter1 = newFilter(getBytes("family1"), getBytes("column1"),
                CompareOp.EQUAL, getBytes("condition1"));
        filter2 = newFilter(getBytes("family2"), getBytes("column1"),
                CompareOp.LESS, getBytes("condition2"));
        filterList.addFilter(filter1);
        filterList.addFilter(filter2);
        if (isPage) {
            filterList.addFilter(new FirstKeyOnlyFilter());
        }
        return filterList;
    }
    private static Filter newFilter(byte[] f, byte[] c, CompareOp op, byte[] v) {
        return new SingleColumnValueFilter(f, c, op, v);
    }
    private static void closeScanner(ResultScanner scanner) {
        if (scanner != null)
            scanner.close();
    }
    /**
     * 封装每行数据
     */
    private static Map<String, String> packRowMap(Map<byte[], byte[]> dataMap) {
        Map<String, String> map = new LinkedHashMap<String, String>();
        for (byte[] key : dataMap.keySet()) {
            byte[] value = dataMap.get(key);
            map.put(toStr(key), toStr(value));
        }
        return map;
    }
    // 根据ROW KEY集合获取GET对象集合 
    private static List<Get> getList(List<byte[]> rowList) {
        List<Get> list = new LinkedList<Get>();
        for (byte[] row : rowList) {
            Get get = new Get(row);
            get.addColumn(getBytes("info"), getBytes("id"));
            get.addColumn(getBytes("info"), getBytes("name"));
            get.addColumn(getBytes("info"), getBytes("email"));
            get.addColumn(getBytes("info"), getBytes("password"));
            list.add(get);
        }
        return list;
    }
    /**
     * 封装配置的所有字段列族
     */
    private static Map<byte[], byte[]> packFamilyMap(Result result) {
        Map<byte[], byte[]> dataMap = null;
        dataMap = new LinkedHashMap<byte[], byte[]>();
        dataMap.putAll(result.getFamilyMap(getBytes("info")));
        return dataMap;
    }
    
    private static String toStr(byte[] bt) {
        return Bytes.toString(bt);
    }
}
