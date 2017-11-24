package com.ilike.sparkproject.dao.factory;

import com.ilike.sparkproject.dao.IAdBlacklistDAO;
import com.ilike.sparkproject.dao.IAdClickTrendDAO;
import com.ilike.sparkproject.dao.IAdProvinceTop3DAO;
import com.ilike.sparkproject.dao.IAdStatDAO;
import com.ilike.sparkproject.dao.IAdUserClickCountDAO;
import com.ilike.sparkproject.dao.IAreaTop3ProductDAO;
import com.ilike.sparkproject.dao.IPageSplitConvertRateDAO;
import com.ilike.sparkproject.dao.ISessionAggrStatDAO;
import com.ilike.sparkproject.dao.ISessionDetailDAO;
import com.ilike.sparkproject.dao.ISessionRandomExtractDAO;
import com.ilike.sparkproject.dao.ITaskDAO;
import com.ilike.sparkproject.dao.ITop10CategoryDAO;
import com.ilike.sparkproject.dao.ITop10SessionDAO;
import com.ilike.sparkproject.dao.impl.AdBlacklistDAOImpl;
import com.ilike.sparkproject.dao.impl.AdClickTrendDAOImpl;
import com.ilike.sparkproject.dao.impl.AdProvinceTop3DAOImpl;
import com.ilike.sparkproject.dao.impl.AdStatDAOImpl;
import com.ilike.sparkproject.dao.impl.AdUserClickCountDAOImpl;
import com.ilike.sparkproject.dao.impl.AreaTop3ProductDAOImpl;
import com.ilike.sparkproject.dao.impl.PageSplitConvertRateDAOImpl;
import com.ilike.sparkproject.dao.impl.SessionAggrStatDAOImpl;
import com.ilike.sparkproject.dao.impl.SessionDetailDAOImpl;
import com.ilike.sparkproject.dao.impl.SessionRandomExtractDAOImpl;
import com.ilike.sparkproject.dao.impl.TaskDAOImpl;
import com.ilike.sparkproject.dao.impl.Top10CategoryDAOImpl;
import com.ilike.sparkproject.dao.impl.Top10SessionDAOImpl;

/**
 * DAO工厂类
 * @author Administrator
 *
 */
public class DAOFactory {


	public static ITaskDAO getTaskDAO() {
		return new TaskDAOImpl();
	}

	public static ISessionAggrStatDAO getSessionAggrStatDAO() {
		return new SessionAggrStatDAOImpl();
	}
	
	public static ISessionRandomExtractDAO getSessionRandomExtractDAO() {
		return new SessionRandomExtractDAOImpl();
	}
	
	public static ISessionDetailDAO getSessionDetailDAO() {
		return new SessionDetailDAOImpl();
	}
	
	public static ITop10CategoryDAO getTop10CategoryDAO() {
		return new Top10CategoryDAOImpl();
	}
	
	public static ITop10SessionDAO getTop10SessionDAO() {
		return new Top10SessionDAOImpl();
	}
	
	public static IPageSplitConvertRateDAO getPageSplitConvertRateDAO() {
		return new PageSplitConvertRateDAOImpl();
	}
	
	public static IAreaTop3ProductDAO getAreaTop3ProductDAO() {
		return new AreaTop3ProductDAOImpl();
	}
	
	public static IAdUserClickCountDAO getAdUserClickCountDAO() {
		return new AdUserClickCountDAOImpl();
	}
	
	public static IAdBlacklistDAO getAdBlacklistDAO() {
		return new AdBlacklistDAOImpl();
	}
	
	public static IAdStatDAO getAdStatDAO() {
		return new AdStatDAOImpl();
	}
	
	public static IAdProvinceTop3DAO getAdProvinceTop3DAO() {
		return new AdProvinceTop3DAOImpl();
	}
	
	public static IAdClickTrendDAO getAdClickTrendDAO() {
		return new AdClickTrendDAOImpl();
	}
	
}
