package com.beef.redisexport.handler;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import redis.clients.jedis.JedisPool;
import MetoXML.Util.ClassFinder;

import com.beef.redisexport.interfaces.IRedisDataHandler;
import com.beef.redisexport.schema.data.KeyDesc;
import com.beef.redisexport.schema.data.KeyFieldDesc;
import com.beef.redisexport.schema.data.KeySchema;
import com.beef.redisexport.schema.util.DBTable;
import com.beef.redisexport.schema.util.MKeySchema;
import com.beef.redisexport.util.DBPool;

public class DefaultRedisDataExportHandler implements IRedisDataHandler {
	private final static Logger logger = Logger.getLogger(DefaultRedisDataExportHandler.class);

	private JedisPool _jedisPool;
	private DBPool _dbPool;
	
	private ConcurrentHashMap<String, DBTable> _tableMap = new ConcurrentHashMap<String, DBTable>();
	
	private MKeySchema _mKeySchema = null;
	
	private String[] _keyPatternArray = null;
	

	@Override
	public void initWithKeySchema(JedisPool jedisPool, DBPool dbPool,
			KeySchema keySchema) {
		_jedisPool = jedisPool;
		_dbPool = dbPool;
		
		if(keySchema != null) {
			_mKeySchema = MKeySchema.convertKeySchema(keySchema);
		}
	}
	
	@Override
	public void initWithKeyPatternArray(JedisPool jedisPool, DBPool dbPool,
			String[] keyPatternArray) {
		_jedisPool = jedisPool;
		_dbPool = dbPool;
		
		_keyPatternArray = keyPatternArray;
	}
	
	@Override
	public void handleRedisKey(String keyPattern, String key, String keyType) {
		logger.debug("handleRedisKey()"
				+ " keyPattern:" + keyPattern + " key:" + key + " keyType:" + keyType);
		
		if(keyType.equals("string")) {
			
		} else if(keyType.equals("list")) {
			
		} else if(keyType.equals("hash")) {
			
		} else if(keyType.equals("set")) {
			throw new RuntimeException("Not support keyType:" + keyType + " of redis yet");
		} else if(keyType.equals("zset")) {
			throw new RuntimeException("Not support keyType:" + keyType + " of redis yet");
		} else {
			throw new RuntimeException("Not support keyType:" + keyType + " of redis yet");
		}
	}
	
	private Object getKeyDescOrKeyFieldDesc(String keyPattern) {
		if(_mKeySchema == null) {
			return null;
		} else {
			return _mKeySchema.getKeyDescMap().get(keyPattern);
		}
	}

}
