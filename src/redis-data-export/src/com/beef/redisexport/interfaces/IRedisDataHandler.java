package com.beef.redisexport.interfaces;

import redis.clients.jedis.JedisPool;

import com.beef.redisexport.schema.data.KeySchema;
import com.beef.redisexport.util.IDBPool;


public interface IRedisDataHandler {

	
	public void initWithKeySchema(JedisPool jedisPool, IDBPool dbPool, KeySchema keySchema);
	
	public void initWithKeyPatternArray(JedisPool jedisPool, IDBPool dbPool, String[] keyPatternArray);
	
	/**
	 * Notice: handleRedisData() will be invoked by multiple threads concurrently.
	 * @param key
	 * @param keyType: could be "string", "list", "hash", "set", "zset"
	 */
	public void handleRedisKey(String key, String keyType);
	
}
