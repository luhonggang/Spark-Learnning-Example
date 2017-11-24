package com.beifeng.dao;

import java.util.List;

import com.beifeng.model.User;

/**
 * 操作user的dao接口
 * 
 * @author gerry
 *
 */
public interface UserDao {
	User getOneUser();

	List<User> getAllUsers();
}
