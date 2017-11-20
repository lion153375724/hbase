package com.learn.hbase.demo.twitBase.service.impl;

import org.apache.hadoop.hbase.util.Bytes;

import com.learn.hbase.common.service.impl.AbstracterHBaseTemplate;

public class RelationService extends AbstracterHBaseTemplate{
	public static final byte[] FOLLOWS_TABLE_NAME = Bytes.toBytes("follows");
	public static final byte[] FOLLOWED_TABLE_NAME = Bytes.toBytes("followedBy");
	public static final byte[] RELATION_FAM = Bytes.toBytes("f");
	public static final byte[] FROM = Bytes.toBytes("from");
	public static final byte[] TO = Bytes.toBytes("to");
}
