package com.learn.hbase.demo.twitBase.model;

import com.learn.hbase.util.ORMHBaseColumn;
import com.learn.hbase.util.ORMHBaseTable;

@ORMHBaseTable(tableName = "user")
public class User {
	@ORMHBaseColumn(family = "info", qualifier = "id")
	private String id;
	@ORMHBaseColumn(family = "info", qualifier = "name")
	private String name;
	@ORMHBaseColumn(family = "info", qualifier = "email")
	private String email;
	@ORMHBaseColumn(family = "info", qualifier = "password")
	private String password;
	@ORMHBaseColumn(family = "info", qualifier = "tweetCount")
	public long tweetCount;

	public User() {
	}

	public User(String id, String name, String email, String password) {
		super();
		this.id = id;
		this.name = name;
		this.email = email;
		this.password = password;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public long getTweetCount() {
		return tweetCount;
	}

	public void setTweetCount(long tweetCount) {
		this.tweetCount = tweetCount;
	}

	@Override
	public String toString() {
		return "User [id=" + id + ", name=" + name + ", email=" + email
				+ ", password=" + password + "]";
	}

}
