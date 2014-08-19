package com.beef.redisexport.util;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;

import com.beef.redisexport.config.DBConfig;

public class DBPool {
	private DataSource _dataSource = null;
	
	public DBPool(DBConfig config) {
		createDataSource(config);
	}
	
	public Connection getConnection() throws SQLException {
		return _dataSource.getConnection();
	}
	
	
	private void createDataSource(DBConfig config) {
		BasicDataSource ds = new BasicDataSource();
		ds.setDriverClassName("com.mysql.jdbc.Driver");
		ds.setUrl(config.getUrl());
		ds.setUsername(config.getUserName());
		ds.setPassword(config.getPassword());
		ds.setInitialSize(config.getMaxActive() / 2);
		ds.setTimeBetweenEvictionRunsMillis(60000);
		ds.setMinEvictableIdleTimeMillis(30000);
		ds.setValidationQuery("select now()");
		ds.setValidationQueryTimeout(3);

		ds.setTestOnBorrow(false);
		ds.setTestWhileIdle(true);
		ds.setTestOnReturn(false);
		
		ds.setRemoveAbandoned(true);
		ds.setRemoveAbandonedTimeout(180);
		
		ds.setMaxActive(config.getMaxActive());
		ds.setMaxIdle(config.getMaxIdle());
		ds.setMaxWait(config.getMaxWait());
		
		if(config.getDefaultAutoCommit() == 0) {
			ds.setDefaultAutoCommit(false);
		} else {
			ds.setDefaultAutoCommit(true);
		}
		
		ds.setDefaultReadOnly(false);
		
		_dataSource = ds;
	}
	
}
