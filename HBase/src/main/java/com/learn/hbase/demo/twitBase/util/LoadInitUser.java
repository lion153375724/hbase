package com.learn.hbase.demo.twitBase.util;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.learn.hbase.demo.twitBase.model.User;
import com.learn.hbase.demo.twitBase.service.impl.UserService;

@Service
public class LoadInitUser {
	@Autowired
	private UserService userService;

	public void initUser() throws Exception {
		int count = 10000;//初始化条数
		List<String> names = LoadUtils.readResource(LoadUtils.NAMES_PATH);
		List<String> words = LoadUtils.readResource(LoadUtils.WORDS_PATH);
		User userBean = null;
		for (int i = 0; i < count; i++) {
			String name = randName(names);
			String id = randUser(name);
			String email = randEmail(id, words);
			userBean = new User(id, name, email, "abc123");
			userService.insert(userBean);
		}

	}
	
	private static String randName(List<String> names) {
		String name = LoadUtils.randNth(names) + " ";
		name += LoadUtils.randNth(names);
		return name;
	}

	private static String randUser(String name) {
		return String
				.format("%s%2d", name.substring(5), LoadUtils.randInt(100));
	}

	private static String randEmail(String user, List<String> words) {
		return String.format("%s@%s.com", user, LoadUtils.randNth(words));
	}
}
