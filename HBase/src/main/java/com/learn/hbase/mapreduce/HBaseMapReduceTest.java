package com.learn.hbase.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

/**
 * 统计ns1:order表中数据的个数，相当于count操作，列=type
 * ns1:order结构：
 * nsq:order cf1 
 *  d90f588c17c641b88ab63d07f99a919e                column=cf1:amount, timestamp=1491819191041, value=430.0                                                                                       
 	d90f588c17c641b88ab63d07f99a919e                column=cf1:createTime, timestamp=1491819191041, value=Mon Apr 10 18:14:07 CST 2017                                                            
 	d90f588c17c641b88ab63d07f99a919e                column=cf1:orderNo, timestamp=1491819191041, value=d90f588c17c641b88ab63d07f99a919e                                                           
 	d90f588c17c641b88ab63d07f99a919e                column=cf1:type, timestamp=1491819191041, value=B  
 * @author jason
 * @createTime 2017年11月15日上午10:43:52
 */
public class HBaseMapReduceTest {
	public static byte[] FAMILY = Bytes.toBytes("cf1");
	public static byte[] COL_NAME = Bytes.toBytes("type");
	
	public static class MyMapper extends TableMapper<Text, IntWritable>{

		Text t = new Text();
		IntWritable i = new IntWritable(1);
		@Override
		protected void map(
				ImmutableBytesWritable key,
				Result value,
				Context context)
				throws IOException, InterruptedException {
			t.set(Bytes.toString(value.getValue(FAMILY, COL_NAME)));
			context.write(t, i);
		}
	}
	
	public static class MyReduce extends TableReducer<Text, IntWritable, NullWritable>{

		public static byte[] FAMILY = Bytes.toBytes("f");
		public static byte[] COL_NAME = Bytes.toBytes("name");
		public static byte[] COL_COUNT = Bytes.toBytes("count");
		@Override
		protected void reduce(
				Text keyin,
				Iterable<IntWritable> valuein,
				Context conext)
				throws IOException, InterruptedException {
			int count = 0;
			for(IntWritable intW : valuein){
				count += intW.get();
			}
			
			Put put = new Put(Bytes.toBytes(keyin.toString()));
			put.addColumn(FAMILY, COL_NAME, Bytes.toBytes(keyin.toString()));
			put.addColumn(FAMILY, COL_COUNT, Bytes.toBytes(count));
			conext.write(NullWritable.get(), put);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration config = HBaseConfiguration.create();
		Job job = Job.getInstance(config);
		job.setJarByClass(HBaseMapReduceTest.class);    // 设置一个运行的主类	            
		Scan scan = new Scan();
		scan.setCaching(500);        // 每批读取多少条数据
		scan.setCacheBlocks(false);  // MapReduce中不要缓存结果，因为只读一次
		// set other scan attrs
		TableMapReduceUtil.initTableMapperJob(
			"ns1:order",      // 从哪个表读数据
			scan,	          // Scan instance to control CF and attribute selection
			MyMapper.class,   // mapper class
			Text.class,	          // mapper output key 跟myMapper对应
			IntWritable.class,	          // mapper output value 跟myMapper对应
			job);
		TableMapReduceUtil.initTableReducerJob(
			"ns1:count",      // output table
			MyReduce.class,             // reducer class
			job);	        
		boolean b = job.waitForCompletion(true);
	}
	
	
}
