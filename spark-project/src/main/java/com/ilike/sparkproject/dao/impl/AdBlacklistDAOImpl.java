package com.ilike.sparkproject.dao.impl;

import com.ilike.sparkproject.dao.IAdBlacklistDAO;
import com.ilike.sparkproject.domain.AdBlacklist;
import com.ilike.sparkproject.jdbc.JDBCHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * 广告黑名单DAO实现类
 * @author Administrator
 *
 */
public class AdBlacklistDAOImpl implements IAdBlacklistDAO {
	
	/**
	 * 批量插入广告黑名单用户
	 * @param adBlacklists
	 */
	@Override
	public void insertBatch(List<AdBlacklist> adBlacklists) {
		String sql = "INSERT INTO ad_blacklist VALUES(?)";
		
		List<Object[]> paramsList = new ArrayList<Object[]>();
		
		for(AdBlacklist adBlacklist : adBlacklists) {
			Object[] params = new Object[]{adBlacklist.getUserid()};
			paramsList.add(params);
		}
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeBatch(sql, paramsList);
	}
	
	/**
	 * 查询所有广告黑名单用户
	 * @return
	 */
	@Override
	public List<AdBlacklist> findAll() {
		String sql = "SELECT * FROM ad_blacklist"; 
		
		final List<AdBlacklist> adBlacklists = new ArrayList<AdBlacklist>();
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		
		jdbcHelper.executeQuery(sql, null, new JDBCHelper.QueryCallback() {
			
			@Override
			public void process(ResultSet rs) throws Exception {
				while(rs.next()) {
					long userid = Long.valueOf(String.valueOf(rs.getInt(1)));  
					
					AdBlacklist adBlacklist = new AdBlacklist();
					adBlacklist.setUserid(userid);  
					
					adBlacklists.add(adBlacklist);
				}
			}
			
		});
		
		return adBlacklists;
	}

}
