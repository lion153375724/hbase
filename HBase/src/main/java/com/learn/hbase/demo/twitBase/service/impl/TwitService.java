package com.learn.hbase.demo.twitBase.service.impl;

import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

import com.learn.hbase.common.service.impl.AbstracterHBaseTemplate;

@Service
public class TwitService extends AbstracterHBaseTemplate{
	public static final byte[] TABLE_NAME = Bytes.toBytes("twits");
	public static final byte[] TWITS_FAM  = Bytes.toBytes("twits");
	
	public static final byte[] TWITS_USER  = Bytes.toBytes("user");
	public static final byte[] TWITS_TEXT  = Bytes.toBytes("text");
}
