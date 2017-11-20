package com.example.demo;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.learn.hbase.HBaseApplication;
import com.learn.hbase.demo.twitBase.util.LoadInitUser;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = HBaseApplication.class)
public class LoadInitUserTest {
	@Autowired
	private LoadInitUser loadInitUser;
	
	@Test
	public void initUser() throws Exception{
		loadInitUser.initUser();
	}
	
	
}
