package com.learn.hbase.demo.weibo.service.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.springframework.stereotype.Service;

import com.learn.hbase.common.service.impl.AbstracterHBaseTemplate;
import com.learn.hbase.demo.weibo.model.UserFriends;
import com.learn.hbase.demo.weibo.service.IUserFriends;

@Service
public class UserFriendsService extends AbstracterHBaseTemplate implements IUserFriends{

	private static byte[] TABLE_NAME = Bytes.toBytes("user_friends");
	private static byte[] TABLE_FAM = Bytes.toBytes("f1");
	
	private static byte[] COL_MYID = Bytes.toBytes("myId");
	private static byte[] COL_FRIENDID = Bytes.toBytes("friendId");
	
	@Override
	public boolean MarkFriend(UserFriends userFriends) {
		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(TABLE_NAME));
			//RowKey=myID[定长=20字节]friendID[定长=20字节]
			byte[] myId = Bytes.toBytes(MD5Hash.getMD5AsHex(Bytes.toBytes(userFriends.getMyId())));
			byte[] friendId = Bytes.toBytes(MD5Hash.getMD5AsHex(Bytes.toBytes(userFriends.getFriendId())));
			byte[] rowKey = Bytes.add(myId,friendId);
			System.out.println(Bytes.toString(rowKey));
			Put put = new Put(rowKey);
			put.addColumn(TABLE_FAM, COL_FRIENDID, Bytes.toBytes(userFriends.getFriendId()));
			table.put(put);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			if(null != table){
				try {
					table.close();
				} catch (Exception e2) {
					e2.printStackTrace();
				}
			}
		}
		return false;
	}

	@Override
	public List<UserFriends> queryFriendByUserId(String userId) {
		//未单独写,取单个rowkey,可以直接用Get方法取
			Scan scan = new Scan();
			Table table = null;
			ResultScanner rs = null;
			List<UserFriends> list = new ArrayList<UserFriends>();
			try {
				table = conn.getTable(TableName.valueOf(TABLE_NAME));
				//方式1：startRow,stopRow
				/*scan.setStartRow(Bytes.toBytes(MD5Hash.getMD5AsHex(Bytes.toBytes(userId))));
				byte[] endRow = Bytes.toBytes(MD5Hash.getMD5AsHex(Bytes.toBytes(userId+"*")));
				scan.setStopRow(endRow);*/
				//方式2：PrefixFilter 前缀过滤器
				PrefixFilter filter = new PrefixFilter(Bytes.toBytes(MD5Hash.getMD5AsHex(Bytes.toBytes(userId))));
				scan.setFilter(filter);
				
				rs = table.getScanner(scan);
				Iterator<Result> iter = rs.iterator();
				Result result = null;
				while(iter.hasNext()){
					result = iter.next();
					UserFriends uf = new UserFriends();
					uf.setMyId(userId);
					uf.setFriendId(Bytes.toString(result.getValue(TABLE_FAM, COL_FRIENDID)));
					list.add(uf);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}finally{
				if(null != table){
					try {
						table.close();
					} catch (Exception e2) {
						e2.printStackTrace();
					}
				}
				
				if(null != rs){
					rs.close();
				}
				
			}
			
			return list;
	}

}
