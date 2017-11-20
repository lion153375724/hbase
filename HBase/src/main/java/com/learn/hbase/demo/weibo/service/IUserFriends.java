package com.learn.hbase.demo.weibo.service;

import java.util.List;

import com.learn.hbase.demo.weibo.model.UserFriends;

public interface IUserFriends {

	/**
	 * 标记为好友
	 * @param userFriends
	 * @return
	 */
	public boolean MarkFriend(UserFriends userFriends);
	
	/**
	 * 
	 * @param userId
	 * @return
	 */
	public List<UserFriends> queryFriendByUserId(String userId);
}
