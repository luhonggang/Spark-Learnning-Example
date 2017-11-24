package com.beifeng.dao.impl;

import java.util.List;

import org.springframework.stereotype.Repository;

import com.beifeng.dao.UserDao;
import com.beifeng.dao.mybatis.MyBatisDao;
import com.beifeng.model.User;

@Repository
public class UserDaoImpl extends MyBatisDao implements UserDao {

	public UserDaoImpl() {
		super(User.class.getName());
		// 此时命名空间为: com.beifeng.model.User
	}

	@Override
	public User getOneUser() {
		int id = 1;
		return super.get("getOneUser", id);
	}

	@Override
	public List<User> getAllUsers() {
		return super.getList("getAllUsers", null);
	}

}
