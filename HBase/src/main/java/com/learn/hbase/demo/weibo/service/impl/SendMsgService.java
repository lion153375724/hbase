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
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.springframework.stereotype.Service;

import com.learn.hbase.common.service.impl.AbstracterHBaseTemplate;
import com.learn.hbase.demo.weibo.model.SendMsg;
import com.learn.hbase.demo.weibo.service.ISendMsg;

@Service
public class SendMsgService extends AbstracterHBaseTemplate implements ISendMsg{

	private static byte[] TABLE_NAME = Bytes.toBytes("user_send_msg");
	private static byte[] TABLE_FAM = Bytes.toBytes("f1");
	
	private static byte[] COL_USERID = Bytes.toBytes("userId");
	private static byte[] COL_CONTENT = Bytes.toBytes("content");
	
	public static void main(String[] args) {
		byte[] start = Bytes.toBytes("532");
		System.out.println(start+":"+start.toString() + ":"+ Bytes.toString(start));
		start[start.length-1]++;
		System.out.println(start+":"+start.toString() + ":"+ Bytes.toString(start));
		System.out.println("bytes:" + Bytes.toBytes("532"));
		System.out.println("md5hash:" + MD5Hash.getMD5AsHex(Bytes.toBytes("532")));
		//md5hash:c4ca4238a0b923820dcc509a6f75849b  1
		//md5hash:298f95e1bf9136124592c8d4825a06fc  532
	}
	@Override
	public boolean sendMsg(SendMsg sendMsg) {
		Table table = null;
		try {
			table = conn.getTable(TableName.valueOf(TABLE_NAME));
			//RowKey=myid[定长]+时间戳
			byte[] rowKey = Bytes.add(Bytes.toBytes(MD5Hash.getMD5AsHex(Bytes.toBytes(sendMsg.getUserId()))),Bytes.toBytes(System.currentTimeMillis()));
			Put put = new Put(rowKey);
			put.addColumn(TABLE_FAM, COL_USERID, Bytes.toBytes(sendMsg.getUserId()));
			put.addColumn(TABLE_FAM, COL_CONTENT, Bytes.toBytes(sendMsg.getContent()));
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
	public List<SendMsg> queryMsgByUserId(String userId) {
		//未单独写,取单个rowkey,可以直接用Get方法取
		Scan scan = new Scan();
		Table table = null;
		ResultScanner rs = null;
		List<SendMsg> list = new ArrayList<SendMsg>();
		try {
			table = conn.getTable(TableName.valueOf(TABLE_NAME));
			SingleColumnValueFilter filter = new SingleColumnValueFilter(TABLE_FAM,COL_USERID,CompareOp.EQUAL,Bytes.toBytes(userId));
			scan.setFilter(filter);
			rs = table.getScanner(scan);
			Iterator<Result> iter = rs.iterator();
			Result result = null;
			while(iter.hasNext()){
				result = iter.next();
				SendMsg msg = new SendMsg();
				msg.setUserId(Bytes.toString(result.getValue(TABLE_FAM, COL_USERID)));
				msg.setContend(Bytes.toString(result.getValue(TABLE_FAM, COL_CONTENT)));
				list.add(msg);
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
