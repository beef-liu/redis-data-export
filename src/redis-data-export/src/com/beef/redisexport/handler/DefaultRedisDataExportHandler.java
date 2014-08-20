package com.beef.redisexport.handler;

import org.apache.log4j.Logger;

import com.beef.redisexport.interfaces.IRedisDataHandler;

public class DefaultRedisDataExportHandler implements IRedisDataHandler {
	private final static Logger logger = Logger.getLogger(DefaultRedisDataExportHandler.class);

	@Override
	public void handleRedisKey(String key, String keyType) {
		logger.debug("handleRedisKey() key:" + key + " keyType:" + keyType);
		
		if(keyType.equals("string")) {
		} else if(keyType.equals("list")) {
		} else if(keyType.equals("hash")) {
		} else if(keyType.equals("set")) {
		} else if(keyType.equals("zset")) {
		} else {
			return;
		}
	}

}
