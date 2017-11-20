package com.learn.hbase.demo.weibo.service;

import java.util.List;

import com.learn.hbase.demo.weibo.model.SendMsg;

public interface ISendMsg {
	/**
	 * 发送消息
	 * @param sendMsg
	 * @return
	 */
	public boolean sendMsg(SendMsg sendMsg);
	
	/**
	 * 查询用户发送的消息
	 * @param userId
	 * @return
	 */
	public List<SendMsg> queryMsgByUserId(String userId);
}
