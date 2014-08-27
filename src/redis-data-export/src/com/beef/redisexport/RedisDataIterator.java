package com.beef.redisexport;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.exceptions.JedisConnectionException;

import com.beef.redisexport.interfaces.IRedisDataHandler;
import com.beef.redisexport.schema.util.KeyPattern;
import com.beef.redisexport.schema.util.KeySchemaUtil;
import com.beef.redisexport.util.DBPool;

public class RedisDataIterator {
	private final static Logger logger = Logger.getLogger(RedisDataIterator.class);
	
	private JedisPool _jedisPool;
	//private DBPool _dbPool;
	
	//private String _keyPattern;
	//private int _iterateLoopCount;
	//private int _threadCount;
	private ScanParams _scanParams;
	
	
	private ScheduledExecutorService _taskPool;
	
	private Object _lockForScanKeys = new Object();
	private IRedisDataHandler _redisDataHandler = null;

	private volatile String _scanCursor = null;
	private volatile ConcurrentHashMap<Integer, Integer> _aliveThreadNumMap = new ConcurrentHashMap<Integer, Integer>();
	private AtomicBoolean _isInShuttingDown = new AtomicBoolean(false);
	
	/*
	private ThreadProgressCallBack _threadProgressCallBack;

	public static interface ThreadProgressCallBack {
		public void onAllThreadStopped();
	}
	*/
	
	public RedisDataIterator(
			JedisPool jedisPool,
			//DBPool dbPool,
			String keyPattern, 
			int threadCount, int scanCount,
			IRedisDataHandler redisDataHandler
			//ThreadProgressCallBack threadProgressCallBack
			) {
		//_dbPool = dbPool;
		_jedisPool = jedisPool;
		
		//_keyPattern = keyPattern;
		//_iterateLoopCount = iterateLoopCount;
		//_threadCount = threadCount;
		
		_redisDataHandler = redisDataHandler;
		
		_scanParams = new ScanParams();
		if(keyPattern != null && keyPattern.length() > 0) {
			KeyPattern pattern = KeySchemaUtil.parseKeyPattern(keyPattern);
			_scanParams.match(pattern.getKeyMatchPattern());
		}
		_scanParams.count(scanCount);
		
		//start threads
		start(threadCount);
	}
	
	private void start(int threadCount) {
		logger.info("RedisDataIterator() start ---------");

		_taskPool = Executors.newScheduledThreadPool(threadCount);
		
		long initialDelay = 1000;
		long period = 10;
		for(int i = 0; i < threadCount; i++) {
			DataIterateTask task = new DataIterateTask(i);
			_taskPool.scheduleAtFixedRate(task, initialDelay, period, TimeUnit.MILLISECONDS);
			
			_aliveThreadNumMap.put(Integer.valueOf(i), Integer.valueOf(i));
		}
	}
	
	/**
	 * It will wait until all threads stop.
	 */
	public void waitForever() {
		try {
			_taskPool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			e.printStackTrace();
			logger.error(null, e);
		}
	}
	
	private void shutDown() {
		boolean isFirstShut = _isInShuttingDown.compareAndSet(false, true);
		if(isFirstShut) {
			logger.info("All thread stopped, then shutdown thread pool --------------");
			_taskPool.shutdown();
			
			/**
			if(_threadProgressCallBack != null) {
				_threadProgressCallBack.onAllThreadStopped();
			}
			*/
		}
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

			if(keyList != null && keyList.size() > 0 && _redisDataHandler != null) {
				logger.debug("task.run() taskNum:" + _taskNum + " keyList.size:" + keyList.size());
				for(int i = 0; i < keyList.size(); i++) {
					handleOneKey(keyList.get(i));
				}
			}
		}
		
		private void handleOneKey(String key) {
			Jedis jedis = null;
			String keyType = null;
			try {
				jedis = _jedisPool.getResource();
				
				keyType = jedis.type(key);
				
				if(keyType == null || keyType.equals("none")) {
					return;
				}
				
			} catch(JedisConnectionException e) {
				_jedisPool.returnBrokenResource(jedis);
				logger.error(null, e);
			} catch(Throwable e) {
				logger.error(null, e);
			} finally {
				try {
					_jedisPool.returnResource(jedis);
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
				String cursor;
				if(_scanCursor == null) {
					cursor = "0";
				} else {
					if(_scanCursor.equals("0")) {
						return null;
					} else {
						cursor = _scanCursor;
					}
				}
				
				jedis = _jedisPool.getResource();
				
				logger.info("scanKey()" + " cursor:" + cursor + " taskNum:" + taskNum);
				ScanResult<String> result = jedis.scan(cursor, _scanParams);
				_scanCursor = result.getStringCursor();
				
				return result.getResult();
			} catch(JedisConnectionException e) {
				_jedisPool.returnBrokenResource(jedis);
				logger.error(null, e);
				
				//shutdown all threads
				shutDown();
				return null;
			} catch(Throwable e) {
				logger.error(null, e);
				return null;
			} finally {
				try {
					_jedisPool.returnResource(jedis);
				} catch(Throwable e) {
					logger.error(null, e);
				}
			}
		}
	} 
	
}
