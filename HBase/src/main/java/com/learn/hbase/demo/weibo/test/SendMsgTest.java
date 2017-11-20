package com.learn.hbase.demo.weibo.test;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.learn.hbase.HBaseApplication;
import com.learn.hbase.demo.weibo.model.SendMsg;
import com.learn.hbase.demo.weibo.service.impl.SendMsgService;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = HBaseApplication.class)
public class SendMsgTest {

	@Autowired
	private SendMsgService sendMsgService;
	
	@Test
	public void insert(){
		SendMsg sendMsg = new SendMsg();
		for(int i = 0;i<10;i++){
			sendMsg.setUserId("jason");
			sendMsg.setContend("jason send msg " + i);
			System.out.println("发送结果:" + sendMsgService.sendMsg(sendMsg));
		}
	}
	
	@Test
	public void query(){
		List<SendMsg> list = sendMsgService.queryMsgByUserId("jason");
		for(SendMsg msg : list){
			System.out.println(msg.getUserId() + ":" + msg.getContent());
		}
	}
}
