package com.beifeng.dao.mybatis;

import java.util.List;

/**
 * 提供具体获取或者操作数据库的接口、方法
 * 
 * @author gerry
 *
 */
public class MyBatisDao extends SqlSessionTemplateDaoSupport {
	private String namespaceName;

	public MyBatisDao() {
	}

	public MyBatisDao(String namespaceName) {
		this.namespaceName = namespaceName;
	}

	/**
	 * 获取mybatis的处理xml元素的定制id
	 * 
	 * @param id
	 * @return
	 */
	private String createStatementName(String id) {
		if (this.namespaceName == null) {
			return id;
		}
		return this.namespaceName + "." + id;
	}

	/**
	 * 获取单个数据
	 * 
	 * @param key
	 * @param params
	 * @return
	 */
	protected <T> T get(String key, Object params) {
		if (params != null) {
			return super.getSqlSession().selectOne(this.createStatementName(key), params);
		} else {
			return super.getSqlSession().selectOne(this.createStatementName(key));
		}
	}

	/**
	 * 获取列表数据
	 * 
	 * @param key
	 * @param params
	 * @return
	 */
	protected <T> List<T> getList(String key, Object params) {
		if (params != null) {
			return super.getSqlSession().selectList(this.createStatementName(key), params);
		} else {
			return super.getSqlSession().selectList(this.createStatementName(key));
		}
	}
}
