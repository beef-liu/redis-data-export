package com.beef.redisexport.config;

public class DBConfig {
	private String _url = "";
	
	private String _userName = "";
	
	private String _password = "";
	
	private int _maxActive = 2;
	
	private int _maxIdle = 1;
	
	private int _maxWait = 30000;
	
	private int _defaultAutoCommit = 0;

	public String getUrl() {
		return _url;
	}

	public void setUrl(String url) {
		_url = url;
	}

	public String getUserName() {
		return _userName;
	}

	public void setUserName(String userName) {
		_userName = userName;
	}

	public String getPassword() {
		return _password;
	}

	public void setPassword(String password) {
		_password = password;
	}

	public int getMaxActive() {
		return _maxActive;
	}

	public void setMaxActive(int maxActive) {
		_maxActive = maxActive;
	}

	public int getMaxIdle() {
		return _maxIdle;
	}

	public void setMaxIdle(int maxIdle) {
		_maxIdle = maxIdle;
	}

	public int getMaxWait() {
		return _maxWait;
	}

	public void setMaxWait(int maxWait) {
		_maxWait = maxWait;
	}

	public int getDefaultAutoCommit() {
		return _defaultAutoCommit;
	}

	public void setDefaultAutoCommit(int defaultAutoCommit) {
		_defaultAutoCommit = defaultAutoCommit;
	}
}
