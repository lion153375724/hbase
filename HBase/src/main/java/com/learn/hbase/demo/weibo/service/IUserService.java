package com.learn.hbase.demo.weibo.service;

import com.learn.hbase.demo.weibo.model.User;

public interface IUserService {
	/**
	 * 用户注册 
	 * @param user
	 * @return
	 */
	public boolean insert(User user);
	
	/**
	 * 根据id删除用户
	 * @param rowkey
	 * @return
	 */
	public boolean Delete(String userId);
}
