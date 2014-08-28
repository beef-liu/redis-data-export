package com.beef.redisexport.handler;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.PatternCompiler;
import org.apache.oro.text.regex.Perl5Compiler;
import org.w3c.tools.codec.Base64FormatException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;
import MetoXML.Util.ClassFinder;

import com.beef.redisexport.interfaces.IRedisDataHandler;
import com.beef.redisexport.schema.data.KeyDesc;
import com.beef.redisexport.schema.data.KeySchema;
import com.beef.redisexport.schema.data.ValueDesc;
import com.beef.redisexport.schema.util.DBTable;
import com.beef.redisexport.schema.util.DBTableUtil;
import com.beef.redisexport.schema.util.KeyPattern;
import com.beef.redisexport.schema.util.KeySchemaUtil;
import com.beef.redisexport.schema.util.MKeySchema;
import com.beef.redisexport.util.DBPool;
import com.beef.util.redis.RedisDataUtil;
import com.beef.util.redis.RedisDataUtil.CompressAlgorithm;
import com.beef.util.redis.compress.CompressException;

public class DefaultRedisDataExportHandler implements IRedisDataHandler {
	private final static Logger logger = Logger.getLogger(DefaultRedisDataExportHandler.class);
	
	private JedisPool _jedisPool;
	private DBPool _dbPool;
	
	/**
	 * key:table name value:DBTable
	 */
	private ConcurrentHashMap<String, DBTable> _tableMap = new ConcurrentHashMap<String, DBTable>();
	
	private MKeySchema _mKeySchema = null;
	private List<KeyPattern> _keyPatternArray = new ArrayList<KeyPattern>();
	private List<Pattern> _keyRegexPatternList = new ArrayList<Pattern>();

	@Override
	public void initWithKeySchema(JedisPool jedisPool, DBPool dbPool,
			KeySchema keySchema) {
		_jedisPool = jedisPool;
		_dbPool = dbPool;
		
		_mKeySchema = MKeySchema.convertKeySchema(keySchema);
		
		for(int i = 0; i < keySchema.getKeyDescs().size(); i++) {
			_keyPatternArray.add(KeySchemaUtil.parseKeyPattern(keySchema.getKeyDescs().get(i).getKeyPattern()));
		}

		try {
			initKeyRegexPatternList();

			initDBTables();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("", e);
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public void initWithKeyPatternArray(JedisPool jedisPool, DBPool dbPool,
			String[] keyPatternArray) {
		_jedisPool = jedisPool;
		_dbPool = dbPool;
		
		_mKeySchema = new MKeySchema();
		
		for(int i = 0; i < keyPatternArray.length; i++) {
			_keyPatternArray.add(KeySchemaUtil.parseKeyPattern(keyPatternArray[i]));
		}

		try {
			initKeyRegexPatternList();
			
			initDBTables();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("", e);
			throw new RuntimeException(e);
		}
	}
	
	protected void initKeyRegexPatternList() throws MalformedPatternException {
		Collections.sort(
				_keyPatternArray, 
				new Comparator<KeyPattern>() {

					@Override
					public int compare(KeyPattern o1, KeyPattern o2) {
						if(o1.getKeyMatchPattern().length() > o2.getKeyMatchPattern().length()) {
							return -1;
						} else if(o1.getKeyMatchPattern().length() < o2.getKeyMatchPattern().length()) {
							return 1;
						} else {
							return o1.getKeyMatchPattern().compareTo(o2.getKeyMatchPattern());
						}
					}
		});
		
		for(int i = 0; i < _keyPatternArray.size(); i++) {
			_keyRegexPatternList.add(KeySchemaUtil.compileRegexPattern(_keyPatternArray.get(i)));
		}
	}

	protected void initDBTables() throws SQLException {
		Connection conn = null;
		
		try {
			conn = _dbPool.getConnection();

			List<String> tableNameList = DBTableUtil.getAllTables(conn);
			
			String tableName;
			DBTable dbTable;
			for(int i = 0; i < tableNameList.size(); i++) {
				tableName = tableNameList.get(i);
				
				dbTable = DBTableUtil.getDBTable(conn, tableName);
				
				_tableMap.put(tableName, dbTable);
			}
			
		} finally {
			conn.close();
		}
	}
	
	@Override
	public void handleRedisKey(String key, String keyType) {
		logger.debug("handleRedisKey()" + " key:" + key + " keyType:" + keyType);
		
		if(keyType.equals("string")) {
			String value = null;
			
			Jedis jedis = null;
			try {
				jedis = _jedisPool.getResource();
				
				value = jedis.get(key);
			} catch(JedisConnectionException e) {
				_jedisPool.returnBrokenResource(jedis);
				throw e;
			} finally {
				_jedisPool.returnResource(jedis);
			}
			
			handleRedisKey(key, null, value);
		} else if(keyType.equals("list")) {
			Jedis jedis = null;
			long len = 0;
			try {
				jedis = _jedisPool.getResource();
				
				len = jedis.llen(key);
				
			} catch(JedisConnectionException e) {
				_jedisPool.returnBrokenResource(jedis);
				throw e;
			} finally {
				_jedisPool.returnResource(jedis);
			}

			String value;
			for(long i = 0; i < len; i++) {
				try {
					jedis = _jedisPool.getResource();
					
					value = jedis.lindex(keyType, i);
				} catch(JedisConnectionException e) {
					_jedisPool.returnBrokenResource(jedis);
					throw e;
				} finally {
					_jedisPool.returnResource(jedis);
				}
				
				handleRedisKey(key, null, value);
			}
			
		} else if(keyType.equals("hash")) {
			Set<String> fieldSet = null;
			Jedis jedis = null;
			try {
				jedis = _jedisPool.getResource();
				
				fieldSet = jedis.hkeys(key);
			} catch(JedisConnectionException e) {
				_jedisPool.returnBrokenResource(jedis);
				throw e;
			} finally {
				_jedisPool.returnResource(jedis);
			}
			
			Iterator<String> iterField = fieldSet.iterator();
			String fieldName;
			String value;
			while(iterField.hasNext()) {
				fieldName = iterField.next();
				
				try {
					jedis = _jedisPool.getResource();
					
					value = jedis.hget(key, fieldName);
					
					handleRedisKey(keyType, fieldName, value);
				} catch(JedisConnectionException e) {
					_jedisPool.returnBrokenResource(jedis);
					throw e;
				} finally {
					_jedisPool.returnResource(jedis);
				}
			}
		} else if(keyType.equals("set")) {
			throw new RuntimeException("Not support keyType:" + keyType + " of redis yet");
		} else if(keyType.equals("zset")) {
			throw new RuntimeException("Not support keyType:" + keyType + " of redis yet");
		} else {
			throw new RuntimeException("Not support keyType:" + keyType + " of redis yet");
		}
	}
	
	protected void handleRedisKey(String key, String fieldName, String value) {
		try {
			if(value == null || value.length() == 0) {
				logger.debug("value is null. key:" + key + " fieldName:" + fieldName);
				return;
			}
			
			//analyze keyPattern
			List<String> variateKeyValueList = null;
			KeyPattern keyPattern = null;
			
			for(int i = 0; i < _keyRegexPatternList.size(); i++) {
				keyPattern = _keyPatternArray.get(i);
				variateKeyValueList = KeySchemaUtil.analyzeKeyPattern(
						_keyRegexPatternList.get(i), key);
				if(variateKeyValueList != null) {
					break;
				}
			}
			
			if(variateKeyValueList == null) {
				logger.warn("Not found keyPattern for key:" + key);
				return;
			}
			
			//find keyDesc
			KeyDesc keyDesc = findKeyDesc(keyPattern, key, fieldName, value);
			
			//decode value if compressed
			String decodedValue = null;
			if (keyDesc.getValDesc().getCompressMode() == null) {
				//not compressed
				decodedValue = value;
			} else {
				if(keyDesc.getValDesc().getCompressMode().equals(ValueDesc.COMPRESS_MODE_LZF)) {
					//lzf
					decodedValue = new String(
							RedisDataUtil.decodeStringBytes(value.getBytes(KeySchemaUtil.DefaultCharset), CompressAlgorithm.LZF),
							KeySchemaUtil.DefaultCharset);
				} else if(keyDesc.getValDesc().getCompressMode().equals(ValueDesc.COMPRESS_MODE_GZIP)) {
					//gzip
					decodedValue = new String(
							RedisDataUtil.decodeStringBytes(value.getBytes(KeySchemaUtil.DefaultCharset), CompressAlgorithm.GZIP),
							KeySchemaUtil.DefaultCharset);
				}
			}
			
			//check DB table
			DBTable dbTable = 
		} catch(Throwable e) {
			logger.error(null, e);
		}
	}

	protected KeyDesc findKeyDesc(KeyPattern keyPattern, 
			String key, String fieldName, String value) throws IOException, Base64FormatException, CompressException {
		KeyDesc keyDesc = _mKeySchema.getKeyDesc(keyPattern.getKeyPattern(), fieldName);
		
		if(keyDesc == null) {
			keyDesc = new KeyDesc();
			keyDesc.setKeyPattern(keyPattern.getKeyPattern());
			keyDesc.setFieldName(fieldName);
			keyDesc.setTableName(KeySchemaUtil.parseTableName(keyPattern, fieldName));
			
			ValueDesc valDesc = KeySchemaUtil.findoutValueDesc(keyPattern.getKeyPattern(), key, fieldName, value);
			keyDesc.setValDesc(valDesc);
			
			_mKeySchema.setKeyDesc(keyPattern.getKeyPattern(), fieldName, keyDesc);
		}
		
		return keyDesc;
	}
	
	protected DBTable findDBTable(KeyDesc keyDesc) {
		DBTable dbTable = _tableMap.get(keyDesc.getTableName());
		
		if(dbTable == null) {
			dbTable = KeySchemaUtil.parseDBTable(keyPattern, key, fieldName, value);
			
			if(dbTable == null) {
				return null;
			}
					
		}
		
		return dbTable;
	}

}
