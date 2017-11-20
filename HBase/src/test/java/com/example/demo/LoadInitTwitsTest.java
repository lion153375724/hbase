package com.example.demo;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.learn.hbase.HBaseApplication;
import com.learn.hbase.demo.twitBase.util.LoadInitTwit;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = HBaseApplication.class)
public class LoadInitTwitsTest {
	@Autowired
	private LoadInitTwit loadInitTwit;
	
	@Test
	public void initTwit() throws Exception{
		loadInitTwit.initTwit();;
	}
	
	
}
