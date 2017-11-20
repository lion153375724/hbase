基于HBase的朋友圈开发
	需求分析:
		1、用户注册
		     1)用户名是否唯一 2)注册时间 3）注册地址 4）个人基本信息
		       原子操作
		2、关注好友
		      MYID_FRIENDID=RowKey
		3、发送消息
		     Put
		4、查看消息
		get scan、看我的文章的点击量
	表设计
		用户注册表
			RowKey=用户名即可（英文）
			列族=f
			列：userId,name,email
		关注好友表(user_friends):我关注了哪些好友 （高表|宽表）
				RowKey=myID[定长=20字节]friendID[定长=20字节]
				列族f
				列为空
			宽表设计(本例子不适合，本例子采用上面的高表设计)
				RowKey=myid
				所关注的好友的ID是列，值为空
		发送消息（user_send_msg)
			RowKey=myid[定长]+时间戳
			列族f
			列：userId，content。。。。
	模拟朋友圈发消息
		各种维度查询代码编写
		验证结果查询结果

