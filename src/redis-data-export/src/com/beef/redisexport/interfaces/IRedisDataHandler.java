package com.beef.redisexport.interfaces;


public interface IRedisDataHandler {

	/**
	 * Notice: handleRedisData() will be invoked by multiple threads concurrently.
	 * @param key
	 * @param keyType: could be "string", "list", "hash", "set", "zset"
	 */
	public void handleRedisKey(String key, String keyType);
	
}
