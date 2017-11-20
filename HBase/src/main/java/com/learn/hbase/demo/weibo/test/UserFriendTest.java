package com.learn.hbase.demo.weibo.test;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.learn.hbase.HBaseApplication;
import com.learn.hbase.demo.weibo.model.SendMsg;
import com.learn.hbase.demo.weibo.model.UserFriends;
import com.learn.hbase.demo.weibo.service.impl.UserFriendsService;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = HBaseApplication.class)
public class UserFriendTest {

	@Autowired
	private UserFriendsService userFriendsService;
	
	@Test
	public void MarkFriend(){
		for(int i = 5;i<6;i++){
			UserFriends userFriends = new UserFriends();
			userFriends.setMyId("wangsi3");
			userFriends.setFriendId("jason"+i);
			System.out.println("发送结果:" + userFriendsService.MarkFriend(userFriends));
		}
	}
	
	@Test
	public void query(){
		List<UserFriends> list = userFriendsService.queryFriendByUserId("jason");
		for(UserFriends UserFriends : list){
			System.out.println(UserFriends.getMyId() + ":" + UserFriends.getFriendId());
		}
	}
}
