package com.beef.redisexport.schema.util;

import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.exceptions.JedisConnectionException;

import com.beef.redisexport.schema.data.FieldDesc;
import com.beef.redisexport.schema.data.KeyDesc;
import com.beef.redisexport.schema.data.KeyFieldDesc;
import com.beef.redisexport.schema.data.KeySchema;
import com.beef.redisexport.schema.data.ValueDesc;
import com.beef.redisexport.util.DBPool;

public class KeySchemaUtil {
	private final static Logger logger = Logger.getLogger(KeySchemaUtil.class);

	public static KeySchema generateDefaultSchema(
			JedisPool jedisPool, DBPool dbPool,
			String[] keyPatternArray, int scanCount) {
		KeySchema keySchema = new KeySchema();
		
		Object keyDesc;
		for(int i = 0; i < keyPatternArray.length; i++) {
			try {
				keyDesc = generateDefaultKeyDescOrKeyFieldDesc(
						jedisPool, dbPool, 
						keyPatternArray[i], scanCount);
				if(keyDesc == null) {
					continue;
				}
				
				if(KeyFieldDesc.class.isAssignableFrom(keyDesc.getClass())) {
					keySchema.getKeyFieldDescs().add((KeyFieldDesc)keyDesc);
				} else {
					keySchema.getKeyDescs().add((KeyDesc)keyDesc);
				}
			} catch(Throwable e) {
				e.printStackTrace();
				logger.error(null, e);
			}
		}
		
		return keySchema;
	}
	
	/**
	 * 
	 * @param keyPattern
	 * @return KeyDesc or KeyFieldDesc
	 */
	public static Object generateDefaultKeyDescOrKeyFieldDesc(
			JedisPool jedisPool, DBPool dbPool,
			String keyPattern, int scanCount) {
		String key = scanKeyPatternFor1Key(jedisPool, keyPattern, scanCount);
		if(key == null) {
			logger.warn("Key does not exists:" + keyPattern);
			return null;
		}
		
		String keyType = getKeyTypeInRedis(jedisPool, key);
		if(keyType == null) {
			return null;
		}
		
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
	
	/**
	 * 
	 * @param jedisPool
	 * @param keyPattern
	 * @param scanCount
	 * @return key
	 */
	public static String scanKeyPatternFor1Key(JedisPool jedisPool, 
			String keyPattern, int scanCount) {
		Jedis jedis = null;
		try {
			ScanParams scanParams = new ScanParams();
			scanParams = new ScanParams();
			if(keyPattern != null && keyPattern.length() > 0) {
				KeyPattern pattern = KeySchemaUtil.parseKeyPattern(keyPattern);
				scanParams.match(pattern.getKeyMatchPattern());
			}
			scanParams.count(scanCount);
			ScanResult<String> result = null;
			String scanCursor = "0";
			
			jedis = jedisPool.getResource();
			while(true) {
				result = jedis.scan(scanCursor, scanParams);
				if(result == null) {
					break;
				}
				
				if(result.getResult().size() > 0) {
					break;
				}
				
				scanCursor = result.getStringCursor();
				if(scanCursor.equals("0")) {
					break;
				}
			}
			
			if(result != null && result.getResult().size() > 0) {
				return result.getResult().get(0);
			} else {
				return null;
			}
		} catch(JedisConnectionException e) {
			jedisPool.returnBrokenResource(jedis);
			throw e;
		} finally {
			jedisPool.returnResource(jedis);
		}
	}
	
	public static String getKeyTypeInRedis(JedisPool jedisPool, String key) {
		Jedis jedis = null;
		String keyType = null;
		try {
			jedis = jedisPool.getResource();
			
			keyType = jedis.type(key);
			
			if(keyType == null || keyType.equals("none")) {
				return null;
			} else {
				return keyType;
			}
		} catch(JedisConnectionException e) {
			jedisPool.returnBrokenResource(jedis);
			throw e;
		} finally {
			jedisPool.returnResource(jedis);
		}
	}
	
	/**
	 * 
	 * @param keyPattern could be test.${userId}.a
	 * @return
	 */
	public static KeyPattern parseKeyPattern(String keyPattern) {
		KeyPattern pattern = new KeyPattern();

		StringBuilder sb = new StringBuilder();
		int beginIndex = 0;
		int index = 0;
		int index2 = 0;
		while(beginIndex <= keyPattern.length()) {
			index = keyPattern.indexOf("${", beginIndex);
			
			if(index < 0) {
				sb.append(keyPattern.substring(beginIndex));
				break;
			}
			
			index2 = keyPattern.indexOf('}', index);
			if(index2 < 0) {
				throw new RuntimeException("Wrong keyPattern:" + keyPattern);
			}
			
			//substring before ${
			sb.append(keyPattern.substring(beginIndex, index));

			//append *
			sb.append('*');
			
			//substring between ${ and }, (variate key name)
			if(pattern.getVariateKeyNames() == null) {
				pattern.setVariateKeyNames(new ArrayList<String>());
			}
			pattern.getVariateKeyNames().add(keyPattern.substring(index + 2, index2).trim());
			
			//set beginIndex after }
			beginIndex = index2 + 1;
		}

		pattern.setKeyMatchPattern(sb.toString());
		
		return pattern;
	}

	/**
	 * test.*.a -> test_a
	 * @param keyPattern
	 * @param key
	 * @param keyDesc
	 * @return Not include nonprimarykey columns.
	 */
	public static DBTable parseDBTable(KeyPattern keyPattern, String key, ValueDesc valDesc) {
		DBTable table = new DBTable();

		String fieldName = null;
		if(valDesc != null && FieldDesc.class.isAssignableFrom(valDesc.getClass())) {
			fieldName = ((FieldDesc)valDesc).getFieldName();
		}
		String tableName = parseTableName(keyPattern, fieldName);
		table.setTableName(tableName);
		table.setComment("from redis:" + keyPattern.getKeyMatchPattern());
		
		String pk;
		for(int i = 0; i < keyPattern.getVariateKeyNames().size(); i++) {
			pk = keyPattern.getVariateKeyNames().get(i);
			table.getPrimaryKeys().add(pk);
			table.getPrimarykeySet().add(pk);
		}
		
		if(valDesc != null) {
			String[] pks = parsePrimaryKeys(valDesc);
			
			if(pks != null) {
				for(int i = 0; i < pks.length; i++) {
					table.getPrimaryKeys().add(pks[i]);
					table.getPrimarykeySet().add(pks[i]);
				}
			}
		}
		
		return table;
	}
	
	private static String[] parsePrimaryKeys(ValueDesc valDesc) {
		if(valDesc.getPrimaryKeysInData() != null && valDesc.getPrimaryKeysInData().length() > 0) {
			return splitStringByComma(valDesc.getPrimaryKeysInData());
		} else {
			return null;
		}
	}
	
	public static String[] splitStringByComma(String strTokens) {
		StringTokenizer stk = new StringTokenizer(strTokens, ",");
		
		String[] strTokenArray = new String[stk.countTokens()];
		int i = 0;
		while(stk.hasMoreTokens()) {
			strTokenArray[i++] = stk.nextToken();
		}
		
		return strTokenArray;
	}
	
	/**
	 * 
	 * @param keyPattern
	 * @param valDesc
	 * @return lowercase
	 */
	public static String parseTableName(KeyPattern keyPattern, String fieldName) {
		StringBuilder sb = new StringBuilder();
		
		char curC;
		for(int i = 0; i < keyPattern.getKeyMatchPattern().length(); i++) {
			curC = keyPattern.getKeyMatchPattern().charAt(i);
			
			if(curC == '*') {
				continue;
			}
			
			if(curC == '.' || curC == '-') {
				if(sb.charAt(sb.length() - 1) != '_') {
					sb.append('_');
				}
			} else {
				sb.append(curC);
			}
		}

		if(fieldName != null && fieldName.length() > 0) {
			if(sb.charAt(sb.length() - 1) != '_') {
				sb.append('_');
			}
			sb.append(fieldName);
		}
		
		return sb.toString().toLowerCase();
	}
	
}
