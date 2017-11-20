package com.learn.hbase.demo.twitBase.util;

import java.util.Date;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.learn.hbase.demo.twitBase.model.Twits;
import com.learn.hbase.demo.twitBase.model.User;
import com.learn.hbase.demo.twitBase.service.impl.TwitService;
import com.learn.hbase.demo.twitBase.service.impl.UserService;

@Service
public class LoadInitTwit {
	@Autowired
	private TwitService twitService;
	@Autowired
	private UserService userService;

	public void initTwit() throws Exception {
		if (!twitService.existTable(Bytes.toString(UserService.TAB_NAME))
				|| !twitService.existTable(Bytes
						.toString(TwitService.TABLE_NAME))) {
			System.out.println("Please use the InitTables utility to create "
					+ "destination tables first.");
			System.exit(0);
		}
		int count = 1;// 每个用户初始化多少条记录
		List<String> words = LoadUtils.readResource(LoadUtils.WORDS_PATH);

		// 取得用户
		List<User> userList = userService.listUser();
		for (User user : userList) {
			for (int i = 0; i < count; i++) {
				Date dt = randDT();
				Twits twit = new Twits(user.getId(),dt, randTwit(words));
				twit.setRowKey(user.getId()+"_"+ dt); //不以id为rowkey以id+dt为rowkey
				twitService.insert(twit);
			}
		}

	}
	

	private static String randTwit(List<String> words) {
		String twit = "";
		for (int i = 0; i < 12; i++) {
			twit += LoadUtils.randNth(words) + " ";
		}
		return twit;
	}

	private static Date randDT() {
		int year = 2010 + LoadUtils.randInt(5);
		int month = 1 + LoadUtils.randInt(12);
		int day = 1 + LoadUtils.randInt(28);
		return new Date(year, month, day, 0, 0, 0);
	}

}
