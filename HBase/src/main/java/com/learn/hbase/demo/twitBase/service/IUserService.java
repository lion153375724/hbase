package com.learn.hbase.demo.twitBase.service;

import java.util.List;

import org.apache.hadoop.hbase.filter.FilterList;

import com.learn.hbase.common.PageModel;
import com.learn.hbase.demo.twitBase.model.User;

public interface IUserService{
	
	/**
	 * 增加用户的tweet总数
	 * @param user
	 * @return
	 */
	public long incTweetCount(String user);
	
	/**
	 * 获取所有的user用户
	 * @return
	 */
	public List<User> listUser();
	
	/**
	 * 根据用户名称查询用户
	 * @param name
	 * @return
	 */
	public List<User> listUserByName(String name);

	/**
	 * 按条件分页查询
	 * @param page
	 * @param tableName
	 * @param startRow
	 * @param stopRow
	 * @param filterList
	 * @param obj
	 * @return
	 */
	public PageModel<User> list_page(PageModel<User> page, String tableName,
			String startRow, String stopRow, FilterList filterList, User obj);

}
