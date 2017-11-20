package com.learn.hbase.util;

import java.util.Date;

@ORMHBaseTable(tableName="ns1:t1")
public class Apple {
	@ORMHBaseColumn(family="rowkey",qualifier="rowkey")
	private String id;
	@ORMHBaseColumn(family="cf4",qualifier="conn")
	private String content;
	@ORMHBaseColumn(family="cf4",qualifier="imgs")
	private String imgs;
	@ORMHBaseColumn(family="cf4",qualifier="createTime")
	private Date createTime;
	public String getId() {
		return id;
	}
	
	public void setId(String id) {
		this.id = id;
	}
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}

	public String getImgs() {
		return imgs;
	}

	public void setImgs(String imgs) {
		this.imgs = imgs;
	}

	public Date getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}
	
}
