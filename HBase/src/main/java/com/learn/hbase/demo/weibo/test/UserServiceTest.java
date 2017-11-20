package com.learn.hbase.demo.weibo.test;

import javax.annotation.Resource;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.learn.hbase.HBaseApplication;
import com.learn.hbase.demo.weibo.model.User;
import com.learn.hbase.demo.weibo.service.impl.UserService;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = HBaseApplication.class)
public class UserServiceTest {

	@Resource(name="wbUserService")
	private UserService userService;
	
	@Test
	public void insert(){
		User user = new User();
		for(int i = 3;i<10;i++){
			user.setUserId("wangsi"+i);
			user.setName("王四"+i);
			user.setEmail("wangsi"+i +"@123.com");
			System.out.println("注册结果:" + userService.insert(user));
		}
	}
}
