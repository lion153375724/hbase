package com.learn.hbase.demo.twitBase.mapreduce;

import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.learn.hbase.demo.twitBase.service.impl.TwitService;
import com.learn.hbase.demo.twitBase.service.impl.UserService;

public class UserTwitCount {
	// map
	public static class Map extends TableMapper<ImmutableBytesWritable, Put> {
		public static enum Counters {
			HAMLET_TAGS
		};

		public static Put mkPut(String username, byte[] fam, byte[] qual,
				byte[] val) {
			Put p = new Put(Bytes.toBytes(username));
			p.addColumn(fam, qual, val);
			return p;
		}

		public void map(ImmutableBytesWritable rowkey, Result result,
				Context context) {
			byte[] b = result.getValue(TwitService.TWITS_FAM,
					TwitService.TWITS_TEXT);
			String msg = Bytes.toString(b); // 取得twit内容
			b = result.getValue(TwitService.TWITS_FAM, TwitService.TWITS_USER);
			String user = Bytes.toString(b);// 取出user用户
			Put p = mkPut(user, TwitService.TWITS_FAM, TwitService.TWITS_TEXT,
					Bytes.toBytes(true));
			ImmutableBytesWritable outkey = new ImmutableBytesWritable(
					p.getRow());
			try {
				context.write(outkey, p);
				context.getCounter(Counters.HAMLET_TAGS).increment(1);
			} catch (Exception e) {
				// gulp!
			}
		}
	}

	// reduce
	public static class Reduce extends
			TableReducer<ImmutableBytesWritable, Put, ImmutableBytesWritable> {
		@Override
		protected void reduce(ImmutableBytesWritable rowkey,
				Iterable<Put> values, Context context) {
			Iterator<Put> i = values.iterator();
			if (i.hasNext()) {
				try {
					context.write(rowkey, i.next());
				} catch (Exception e) {
					// gulp!
				}
			}
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		Job job = new Job(conf, "TwitBase  tagger");
		job.setJarByClass(UserTwitCount.class);

		Scan scan = new Scan();
		scan.addColumn(TwitService.TWITS_FAM, TwitService.TWITS_USER);
		scan.addColumn(TwitService.TWITS_FAM, TwitService.TWITS_TEXT);
		TableMapReduceUtil.initTableMapperJob(
				Bytes.toString(TwitService.TABLE_NAME), // map表名:twit
				scan, // 扫描的数据
				Map.class, // 执行map的类
				ImmutableBytesWritable.class, // 输出的key/value类型是
												// ImmutableBytesWritable和Put类型
				Put.class, // 输出的key/value类型是 ImmutableBytesWritable和Put类型
				job); // 最后一个参数是作业对象

		TableMapReduceUtil.initTableReducerJob(
				Bytes.toString(UserService.TAB_NAME), // reduce表名:user
				Reduce.class, job);

		job.setNumReduceTasks(0);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
