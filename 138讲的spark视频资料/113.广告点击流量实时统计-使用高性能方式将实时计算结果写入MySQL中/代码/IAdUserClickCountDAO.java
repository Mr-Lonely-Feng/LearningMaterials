package com.ibeifeng.sparkproject.dao;

import java.util.List;

import com.ibeifeng.sparkproject.domain.AdUserClickCount;

/**
 * 用户广告点击量DAO接口
 * @author Administrator
 *
 */
public interface IAdUserClickCountDAO {

	void updateBatch(List<AdUserClickCount> adUserClickCounts);
	
}
