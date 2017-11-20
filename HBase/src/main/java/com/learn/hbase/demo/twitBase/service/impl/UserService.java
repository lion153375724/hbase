package com.learn.hbase.demo.twitBase.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.learn.hbase.common.PageModel;
import com.learn.hbase.common.service.impl.AbstracterHBaseTemplate;
import com.learn.hbase.demo.twitBase.model.User;
import com.learn.hbase.demo.twitBase.service.IUserService;
import com.learn.hbase.util.HBaseUtil;

@Service
public class UserService extends AbstracterHBaseTemplate implements IUserService{
	private Logger logger = LoggerFactory
			.getLogger(UserService.class);
	public static final byte[] TAB_NAME = Bytes.toBytes("user");
	public static final byte[] TAB_FAMILY = Bytes.toBytes("info");
	
	public static final byte[] TAB_TWEETCOUNT = Bytes.toBytes("tweetCount");

	final byte[] POSTFIX = new byte[] { 0x00 };  
	
	@Override
	public List<User> listUser() {
		ResultScanner rs = null;
		Table table = null;
		List<User> results = new ArrayList<User>();
		try {
			Scan scan = new Scan();
	    	table = conn.getTable(TableName.valueOf(TAB_NAME));
	    	rs = table.getScanner(scan);
	    	results = ResultScannerToList(rs);
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					logger.warn("", e);
				}
			}
			if (rs != null){
				rs.close();
			}
		}
		
		return results;
	}
	
	/**
	 * 把resultScanner结果转换为List<User>
	 * @param rs
	 * @return
	 */
	private List<User> ResultScannerToList(ResultScanner rs){
		Iterator<Result> it = rs.iterator();
		List<User> results = new ArrayList<User>();
    	while(it.hasNext()){
    		Result rsl = it.next();
			try {
				User bean= HBaseUtil.result2Bean(rsl, new User());
				User bean2 = new User(bean.getId(), bean.getName(),bean.getEmail(),bean.getPassword());
				results.add(bean2);
			} catch (Exception e) {
				logger.warn("", e);
			}
    	}
		return results;
	}

	public void updateUserName(String id,String oldName,String newName){
		Table table = null;
		try {
			table =  conn.getTable(TableName.valueOf(TAB_NAME));
	    	Put put = new Put(Bytes.toBytes(id));
	    	put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(newName));
	    	//如果对应值为空则进行上面定义的put
	    	table.checkAndPut(Bytes.toBytes(id), Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(oldName), put);
	    	System.out.println("update success!");
	    	table.close();
		} catch (Exception e) {
			// TODO: handle exception
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
	
	@Override
	public List<User> listUserByName(String name) {
		ResultScanner rs = null;
		Table table = null;
		List<User> results = new ArrayList<User>();
		try {
			Scan scan = new Scan();
	    	table = conn.getTable(TableName.valueOf(TAB_NAME));
	    	SingleColumnValueFilter ft1 = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"),CompareOp.EQUAL,Bytes.toBytes(name));
	    	scan.setFilter(ft1);
	    	rs = table.getScanner(scan);
	    	results = ResultScannerToList(rs);
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					logger.warn("", e);
				}
			}
			if (rs != null){
				rs.close();
			}
		}
		return results;
	}
	
	@Override
	public PageModel<User> list_page(PageModel<User> page, String tableName,
			String startRow, String stopRow, FilterList filterList,User obj) {
		ResultScanner scanner = null;
		Table table = null;
		PageModel<User> pageModel = new PageModel<User>();
		List<User> objs = new ArrayList<User>();
		// 为分页创建的封装类对象，下面有给出具体属性
		try {

			// 从表池中取出HBASE表对象
			table = conn.getTable(TableName.valueOf(tableName));
			// 获取筛选对象
			Scan scan = new Scan();
			scan.setStartRow(Bytes.toBytes(startRow));
			scan.setStopRow(Bytes.toBytes(stopRow));
			// 给筛选对象放入过滤器(true标识分页,具体方法在下面)
			//scan.setFilter(filterList);
			// 缓存1000条数据
			scan.setCaching(1000);
			scan.setCacheBlocks(false);
			scanner = table.getScanner(scan);
	    	
			Iterator<Result> it = scanner.iterator();
			int i = 0;
			Integer firstPage = (page.getPageNo() - 1) * page.getPageSize();
			Integer endPage = firstPage + page.getPageSize();
			while (it.hasNext()) {
				Result result = it.next();
				if (result == null || result.isEmpty()) {
					continue;
				}
				if (i >= firstPage && i < endPage) {
					//取出rowkey
					String rowkey = Bytes.toString(result.getRow());
					try {
						//根据rowkey取得对象
						User bean= this.get(new User(), rowkey).get(0);
						objs.add(bean);
					} catch (Exception e) {
						logger.warn("", e);
					}
				}
				i++;
			}

			// 封装分页对象
			pageModel.setPageNo(page.getPageNo());
			pageModel.setPageSize(page.getPageSize());
			pageModel.setRowCount(i);
			pageModel.setDatas(objs);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					logger.warn("", e);
				}
			}
			if (scanner != null) {
				scanner.close();
			}
		}

		return pageModel;
	}

	@Override
	public long incTweetCount(String id) {
		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(TAB_NAME));
			return table.incrementColumnValue(TAB_NAME, TAB_FAMILY, TAB_TWEETCOUNT, 1L);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return 0;
	}

}
