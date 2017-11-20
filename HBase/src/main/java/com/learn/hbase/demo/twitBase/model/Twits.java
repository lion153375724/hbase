package com.learn.hbase.demo.twitBase.model;

import java.util.Date;

import com.learn.hbase.util.ORMHBaseColumn;
import com.learn.hbase.util.ORMHBaseTable;

@ORMHBaseTable(tableName = "twits")
public class Twits {
	public String rowKey ; //如果没有id字段。不以id为rowkey的话，设置此字段值
	@ORMHBaseColumn(family = "twits", qualifier = "user")
	public String user;
	@ORMHBaseColumn(family = "twits", qualifier = "dt")
	public Date dt;
	@ORMHBaseColumn(family = "twits", qualifier = "text")
	public String text;

	public Twits(String user, Date dt, String text) {
		super();
		this.user = user;
		this.dt = dt;
		this.text = text;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public Date getDt() {
		return dt;
	}

	public void setDt(Date dt) {
		this.dt = dt;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}
//
	@Override
	public String toString() {
		return String.format("<Twit: %s %s %s>", user, dt, text);
	}

	public String getRowKey() {
		return rowKey;
	}

	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}
}
