package com.beef.redisexport;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.exceptions.JedisConnectionException;

import com.beef.redisexport.interfaces.IRedisDataHandler;

public class RedisDataIterator {
	private final static Logger logger = Logger.getLogger(RedisDataIterator.class);
	
	private String _keyPattern;
	//private int _iterateLoopCount;
	//private int _threadCount;
	private ScanParams _scanParams;
	
	
	private ScheduledExecutorService _taskPool;
	
	private Object _lockForScanKeys = new Object();
	private IRedisDataHandler _redisDataHandler = null;

	private volatile String _scanCursor = null;
	private volatile ConcurrentHashMap<Integer, Integer> _aliveThreadNumMap = new ConcurrentHashMap<Integer, Integer>();
	
	public RedisDataIterator(
			String keyPattern, 
			int threadCount, int scanCount,
			IRedisDataHandler redisDataHandler) {
		_keyPattern = keyPattern;
		//_iterateLoopCount = iterateLoopCount;
		//_threadCount = threadCount;
		
		_redisDataHandler = redisDataHandler;
		
		_scanParams = new ScanParams();
		_scanParams.match(keyPattern);
		_scanParams.count(scanCount);
		
		//create threads
		_taskPool = Executors.newScheduledThreadPool(threadCount);
		
		long initialDelay = 1000;
		long period = 10;
		for(int i = 0; i < threadCount; i++) {
			DataIterateTask task = new DataIterateTask(i);
			_taskPool.scheduleAtFixedRate(task, initialDelay, period, TimeUnit.MILLISECONDS);
			
			_aliveThreadNumMap.put(Integer.valueOf(i), Integer.valueOf(i));
		}
	}

	private void shutDown() {
		_taskPool.shutdown();
	}
	
	protected class DataIterateTask implements Runnable {
		private int _taskNum;
		
		public DataIterateTask(int taskNum) {
			_taskNum = taskNum;
		}

		@Override
		public void run() {
			if(_aliveThreadNumMap.size() == 0) {
				shutDown();
			}
			
			if(_scanCursor != null && _scanCursor.equals("0")) {
				_aliveThreadNumMap.remove(Integer.valueOf(_taskNum));
			}

			List<String> keyList = scanKey(_taskNum);

			if(keyList != null && _redisDataHandler != null) {
				logger.info("DataIterateTask() taskNum:" + _taskNum + " keyList.size:" + keyList.size());
				
				for(int i = 0; i < keyList.size(); i++) {
					handleOneKey(keyList.get(i));
				}
			}
		}
		
		private void handleOneKey(String key) {
			Jedis jedis = null;
			String keyType = null;
			try {
				jedis = RedisDataExportContext.singleton().getJedisPool().getResource();
				
				keyType = jedis.type(key);
				
				if(keyType == null || keyType.equals("none")) {
					return;
				}
				
			} catch(JedisConnectionException e) {
				RedisDataExportContext.singleton().getJedisPool().returnBrokenResource(jedis);
				logger.error(null, e);
			} catch(Throwable e) {
				logger.error(null, e);
			} finally {
				try {
					RedisDataExportContext.singleton().getJedisPool().returnResource(jedis);
				} catch(Throwable e) {
					logger.error(null, e);
				}
			}

			
			try {
				_redisDataHandler.handleRedisKey(key, keyType);
			} catch(Throwable e) {
				logger.error(null, e);
			}
		}
	}
	
	
	private List<String> scanKey(int taskNum) {
		synchronized (_lockForScanKeys) {
			Jedis jedis = null;

			try {
				jedis = RedisDataExportContext.singleton().getJedisPool().getResource();

				String cursor;
				if(_scanCursor == null) {
					cursor = "0";
				} else {
					cursor = _scanCursor;
				}
				
				logger.info("scanKey() keyPattern:" + _keyPattern + " cursor:" + cursor + " taskNum:" + taskNum);
				ScanResult<String> result = jedis.scan(cursor, _scanParams);
				_scanCursor = result.getStringCursor();
				
				return result.getResult();
			} catch(JedisConnectionException e) {
				RedisDataExportContext.singleton().getJedisPool().returnBrokenResource(jedis);
				logger.error(null, e);
				return null;
			} catch(Throwable e) {
				logger.error(null, e);
				return null;
			} finally {
				try {
					RedisDataExportContext.singleton().getJedisPool().returnResource(jedis);
				} catch(Throwable e) {
					logger.error(null, e);
				}
			}
		}
	} 
	
}
