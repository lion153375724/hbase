package com.learn.hbase.demo.weibo.service.impl;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

import com.learn.hbase.common.service.impl.AbstracterHBaseTemplate;
import com.learn.hbase.demo.weibo.model.User;
import com.learn.hbase.demo.weibo.service.IUserService;

@Service(value="wbUserService")
public class UserService extends AbstracterHBaseTemplate implements IUserService{

	private static byte[] TABLE_NAME = Bytes.toBytes("user");
	private static byte[] TABLE_FAM = Bytes.toBytes("f1");
	
	private static byte[] COL_NAME = Bytes.toBytes("name");
	private static byte[] COL_EMAIL = Bytes.toBytes("email");
	
	@Override
	public boolean insert(User user) {
		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(TABLE_NAME));
			Put put = new Put(Bytes.toBytes(user.getUserId()));
			put.addColumn(TABLE_FAM, COL_NAME, Bytes.toBytes(user.getName()));
			put.addColumn(TABLE_FAM, COL_EMAIL, Bytes.toBytes(user.getEmail()));
			//table.put(put);
			//return true;
			return table.checkAndPut(Bytes.toBytes(user.getUserId()), TABLE_FAM, COL_NAME, null, put);
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
		}finally{
			if(null != table){
				try {
					table.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return false;
	}

	@Override
	public boolean Delete(String userId) {
		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(TABLE_NAME));
			Delete del = new Delete(Bytes.toBytes(userId)); //rowkey
			table.delete(del);
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
		}
		return false;
	}

}
