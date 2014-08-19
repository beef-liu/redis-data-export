package com.beef.redisexport.config;

public class RedisConfig {
	private String _host = "127.0.0.1";
	private int _port = 6379;
	private int _maxIdle = 10;
	private int _maxTotal = 1024;
	private long _maxWaitMillis = 500;
	private long _softMinEvictableIdleTimeMillis = 10000;
	private boolean _testOnBorrow = false;
	
	public String getHost() {
		return _host;
	}
	public void setHost(String host) {
		_host = host;
	}
	public int getPort() {
		return _port;
	}
	public void setPort(int port) {
		_port = port;
	}
	public int getMaxIdle() {
		return _maxIdle;
	}
	public void setMaxIdle(int maxIdle) {
		_maxIdle = maxIdle;
	}
	public int getMaxTotal() {
		return _maxTotal;
	}
	public void setMaxTotal(int maxTotal) {
		_maxTotal = maxTotal;
	}
	public long getMaxWaitMillis() {
		return _maxWaitMillis;
	}
	public void setMaxWaitMillis(long maxWaitMillis) {
		_maxWaitMillis = maxWaitMillis;
	}
	public long getSoftMinEvictableIdleTimeMillis() {
		return _softMinEvictableIdleTimeMillis;
	}
	public void setSoftMinEvictableIdleTimeMillis(
			long softMinEvictableIdleTimeMillis) {
		_softMinEvictableIdleTimeMillis = softMinEvictableIdleTimeMillis;
	}
	public boolean isTestOnBorrow() {
		return _testOnBorrow;
	}
	public void setTestOnBorrow(boolean testOnBorrow) {
		_testOnBorrow = testOnBorrow;
	}
}
