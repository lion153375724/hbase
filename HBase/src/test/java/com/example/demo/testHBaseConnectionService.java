package com.example.demo;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.learn.hbase.HBaseApplication;
import com.learn.hbase.common.service.impl.AbstracterHBaseTemplate;
import com.learn.hbase.util.Apple;
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = HBaseApplication.class)
public class testHBaseConnectionService {
	@Autowired
	private AbstracterHBaseTemplate hBaseConnectionService;
	
	@Test
	public void testAddFal(){
		hBaseConnectionService.addColumnFamilys("ns1:t1", "f4");
	}
	
	@Test
	public void listFamilys(){
		List<String>  sList = hBaseConnectionService.familys("ns1:t1");
		for(String str : sList){
			System.out.println(str);
		}
	}
	
	@Test
	public void listTable(){
		List<String>  sList = hBaseConnectionService.tables();
		for(String str : sList){
			System.out.println(str);
		}
	}
	
	@Test
	public void testInsert(){
		Apple apple = new Apple();
		apple.setId("t1234");
		apple.setContent("gaoweigong");
		apple.setImgs("http://StringTest.jpg");
		apple.setCreateTime(new Date());
		hBaseConnectionService.insert(apple);
	}
	
	@Test
	public void testDelete(){
		hBaseConnectionService.delete("crawldbtest", "123");
	}
	
	@Test
	public void testGet(){
		List<Apple> objs = hBaseConnectionService.get(new Apple(), "t1234");
		for (Apple obj : objs) {
			System.out.println(obj.getContent());
			System.out.println(obj.getId());
			System.out.println(obj.getImgs().toString());
			System.out.println(obj.getCreateTime());
			System.out.println(dateToString(obj.getCreateTime()));
		}
	}
	
	private String dateToString(Date date){
		if(null != date){
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			return sdf.format(date);
		}
		return null;
	}
	
	public static void main(String[] args) throws ParseException {
		String dateString = "Wed Nov 08 09:34:19 CST 2017";
		SimpleDateFormat sfEnd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat sfStart = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy",java.util.Locale.ENGLISH) ;
		Date d = sfStart.parse(dateString);
		System.out.println(d);
		System.out.println(sfEnd.format(d));
	}
}
