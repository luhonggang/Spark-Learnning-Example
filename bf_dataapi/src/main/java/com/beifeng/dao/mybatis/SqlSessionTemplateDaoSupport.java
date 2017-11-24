package com.beifeng.dao.mybatis;

import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * 提供mybatis的session的一个基础类
 * 
 * @author gerry
 *
 */
public class SqlSessionTemplateDaoSupport {
	protected SqlSessionTemplate sqlSession;
	protected boolean externalSqlSession;

	/**
	 * 外部直接给定sqlsession对象
	 * 
	 * @param sqlSessionTemplate
	 */
	public void setSqlSessionTemplate(SqlSessionTemplate sqlSessionTemplate) {
		this.sqlSession = sqlSessionTemplate;
		this.externalSqlSession = true;
	}

	/**
	 * spring注入session factory对象
	 * 
	 * @param sqlSessionFactory
	 */
	@Autowired(required = false)
	public void setSqlSessionFactory(SqlSessionFactory sqlSessionFactory) {
		if (!this.externalSqlSession) {
			this.sqlSession = new SqlSessionTemplate(sqlSessionFactory);
		}
	}

	/**
	 * 返回支持batch插入的mybatis回话
	 * 
	 * @return
	 */
	public final SqlSession getBatchSqlSession() {
		return new SqlSessionTemplate(this.sqlSession.getSqlSessionFactory(), ExecutorType.BATCH);
	}

	/**
	 * 返回普通回话
	 * 
	 * @return
	 */
	public final SqlSession getSqlSession() {
		return this.sqlSession;
	}
}
