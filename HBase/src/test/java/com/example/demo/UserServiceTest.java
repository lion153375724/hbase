package com.example.demo;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.learn.hbase.HBaseApplication;
import com.learn.hbase.common.HBasePageModel;
import com.learn.hbase.common.PageModel;
import com.learn.hbase.demo.twitBase.model.User;
import com.learn.hbase.demo.twitBase.service.impl.UserService;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = HBaseApplication.class)
public class UserServiceTest {
	@Autowired
	private UserService userService;
	
	@Test
	public void initTables(){
		
	}
	
	@Test
	public void testInsert(){
		for(int i=3;i<10;i++){
			User user = new User();
			user.setId("user"+i);
			user.setName("jsaon"+i);
			user.setEmail("123@123.com."+i);
			user.setPassword("pwd"+i);
			userService.insert(user);
		}
	}
	
	@Test
	public void testUpdate(){
		userService.updateUserName("user4","jsaon4","jsaon5");
	}
	
	@Test
	public void delete(){
		userService.delete("user", "jsaon4");
	}
	
	@Test
	public void listUser(){
		List<User> users = userService.listUser();
		for(User user : users){
			System.out.println(user.getId() +":" + user.getName() +":" + user.getEmail() +":"+ user.getPassword());
		}
	}
	
	@Test
	public void listUserByName(){
		List<User> users = userService.listUserByName("jsaon5");
		for(User user : users){
			System.out.println(user.getId() +":" + user.getName() +":" + user.getEmail() +":"+ user.getPassword());
		}
	}
	
	@Test
	public void list_page(){
		PageModel<User> page = new PageModel<User>();
		page.setPageNo(2);
		page.setPageSize(2);
		FilterList filterList = new FilterList();
		//得每条数据的第一个kv，可以用于count，计算总数，速度很快,s.setCaching(500);s.setCacheBlocks(false);这三个参数，
		//否则速度会降下来很多 总的来说，可以节省很多时间
		filterList.addFilter(new FirstKeyOnlyFilter()); 
		PageModel<User> pageModel = userService.list_page(page, "user", "user1:", "user9:", filterList,new User());
		System.out.println(pageModel.getPageNo()+":"+pageModel.getPageSize()+":"+pageModel.getRowCount()+":"+pageModel.getTotalPages());
		List<User> list = pageModel.getDatas();
		for(User user : list){
			System.out.println(user.getId() +":" + user.getName() +":" + user.getEmail() +":"+ user.getPassword());
		}
	}
	
	@Test
	public void getDataMap() throws IOException{
		 String startRow = "user1";
	        String stopRow = "user9";
	        int currentPage = 2;
	        int pageSize = 2;
	        // 执行hbase查询
	        HBasePageModel page = userService.getDataMap("user", startRow, stopRow, currentPage, pageSize);
	        System.out.println(page.getCurrentPage()+":"+page.getTotalCount()+":"+page.getTotalPage());
	        List<Map<String, String>> list = page.getResultList();
	        for(Map<String,String> map : list){
	        	System.out.println(map.get("id")+":"+map.get("name")+":"+map.get("email")+":"+map.get("password"));
	        }
	}
}
