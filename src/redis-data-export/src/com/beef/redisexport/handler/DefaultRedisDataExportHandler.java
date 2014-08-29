package com.beef.redisexport.handler;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;
import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.PatternCompiler;
import org.apache.oro.text.regex.Perl5Compiler;
import org.w3c.tools.codec.Base64FormatException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;
import MetoXML.XmlReader;
import MetoXML.Base.XmlNode;
import MetoXML.Base.XmlParseException;
import MetoXML.Util.ClassFinder;

import com.beef.redisexport.interfaces.IRedisDataHandler;
import com.beef.redisexport.schema.data.KeyDesc;
import com.beef.redisexport.schema.data.KeySchema;
import com.beef.redisexport.schema.data.ValueDesc;
import com.beef.redisexport.schema.util.DBCol;
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
			
			outputLogOfKeyPattern(_keyPatternArray.get(i));
		}
	}
	
	private static void outputLogOfKeyPattern(KeyPattern keyPattern) {
		logger.info("initKeyRegexPatternList() keyPattern:" + keyPattern.getKeyPattern() + " keyMatchPattern:" + keyPattern.getKeyMatchPattern());
		
		StringBuilder sb = new StringBuilder();
		sb.append(" key variable names:");
		for(int k = 0; k < keyPattern.getVariateKeyNames().size(); k++) {
			if(k > 0) {
				sb.append(",");
			}
			sb.append(keyPattern.getVariateKeyNames().get(k));
		}
		logger.info("initKeyRegexPatternList() " + sb.toString());
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
				
				logger.info("initDBTables() load table info from DB:" + tableName);
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
				logger.debug("handleRedisKey() value is null. key:" + key + " fieldName:" + fieldName);
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
				logger.warn("handleRedisKey() Not found keyPattern for key:" + key);
				return;
			}
			
			logger.debug("handleRedisKey()" 
					+ " keyPattern:" + keyPattern.getKeyPattern() 
					+ " keyMatchPattern:" + keyPattern.getKeyMatchPattern());
			
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
			DBTable dbTable = findDBTable(keyDesc, keyPattern, fieldName, decodedValue);
			if(dbTable == null) {
				logger.warn("handleRedisKey() Not enough information to create DB table." 
						+ " keyPattern:" + keyPattern.getKeyPattern() 
						+ " key:" + key + " fieldName:" + fieldName + " decodedValue:" + decodedValue);
				return;
			}
	
			//insert or update into table
			int updCnt = insertValueIntoDBTable(
					dbTable, keyPattern, variateKeyValueList, 
					keyDesc.getValDesc(), decodedValue);
			logger.info("handleRedisKey() updCnt:" + updCnt);
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
	
	protected DBTable findDBTable(KeyDesc keyDesc, KeyPattern keyPattern, String fieldName, String value) 
			throws IOException, XmlParseException, SQLException {
		DBTable dbTable = _tableMap.get(keyDesc.getTableName());
		
		if(dbTable == null) {
			//parse DBTable
			dbTable = KeySchemaUtil.parseDBTable(
					keyPattern, fieldName, value, keyDesc.getValDesc().isDataXml()
					);
			
			if(dbTable == null) {
				return null;
			}
			
			//create table
			Connection conn = null;
			try {
				conn = _dbPool.getConnection();
				
				DBTableUtil.createTable(conn, dbTable);
				
				logger.info("findDBTable() create DB table:" + dbTable.getTableName());
			} finally {
				conn.close();
			}
			
			//add into map
			_tableMap.put(dbTable.getTableName(), dbTable);
		}
		
		return dbTable;
	}

	protected int insertValueIntoDBTable(
			DBTable dbTable, 
			KeyPattern keyPattern, List<String> variateKeyValueList,
			ValueDesc valDesc, String value 
			) throws SQLException {
		int updCnt = 0;
		
		Connection conn = null;
		try {
			conn = _dbPool.getConnection();
			conn.setAutoCommit(true);
			
			List<String> primarykeyValueList = new ArrayList<String>();
			primarykeyValueList.addAll(variateKeyValueList);
			
			if(valDesc.isDataXml()) {
				List<String> otherColValueList = new ArrayList<String>();

				XmlReader xmlReader = new XmlReader();
				XmlNode rootNode = xmlReader.StringToXmlNode(value, KeySchemaUtil.DefaultCharset);
				
				if(rootNode.getName().equalsIgnoreCase("list")) {
					XmlNode dataNode = rootNode.getFirstChildNode();
					if(dataNode == null) {
						//not handle when there is not data in list
						return 0;
					}
					
					//it is a List, then there is "row_num"
					primarykeyValueList.add("0");
					
					String colName;
					String colVal;
					int index = 0;
					HashMap<String, String> colValMap = new HashMap<String, String>();
					while(dataNode != null) {
						XmlNode colNode = dataNode.getFirstChildNode();
						primarykeyValueList.set(primarykeyValueList.size() - 1, String.valueOf(index + 1));
						
						//one row
						try {
							colValMap.clear();
							otherColValueList.clear();
							
							while(colNode != null) {
								colName = colNode.getName().toLowerCase();
								colVal = colNode.getContent();
								
								//col must not in primary keys
								if(dbTable.getPrimaryKey(colName) == null) {
									colValMap.put(colName, colVal);
								}
								
								colNode = colNode.getNextNode();
							}
							
							for(int i = 0; i < dbTable.countOfCols(); i++) {
								colName = dbTable.getCol(i).getColName();
								
								colVal = colValMap.get(colName);
								otherColValueList.add(colVal);
								
								//check col
								DBCol dbCol = checkColumnExists(conn, dbTable, colName, colVal);
								checkColumnMaxLength(conn, dbTable, dbCol, colVal);
							}
							
							//insert or update
							updCnt += DBTableUtil.insertOrUpdate(conn, 
									dbTable.getTableName(), 
									dbTable.getPrimaryKeys(), primarykeyValueList, 
									dbTable.getCols(), otherColValueList);
						} catch(Throwable e) {
							logger.error(null, e);
						}
						
						dataNode = dataNode.getNextNode();
						index++;
					}
				} else {
					XmlNode colNode = rootNode.getFirstChildNode();
					String colName;
					String colVal;
					
					HashMap<String, String> colValMap = new HashMap<String, String>();
					//colValMap.clear();
					otherColValueList.clear();
					
					while(colNode != null) {
						colName = colNode.getName().toLowerCase();
						colVal = colNode.getContent();
						
						//col must not in primary keys
						if(dbTable.getPrimaryKey(colName) == null) {
							colValMap.put(colName, colVal);
						}
						
						colNode = colNode.getNextNode();
					}
					
					for(int i = 0; i < dbTable.countOfCols(); i++) {
						colName = dbTable.getCol(i).getColName();
						
						colVal = colValMap.get(colName);
						otherColValueList.add(colVal);
						
						//check col
						DBCol dbCol = checkColumnExists(conn, dbTable, colName, colVal);
						checkColumnMaxLength(conn, dbTable, dbCol, colVal);
					}
					
					//insert or update
					updCnt += DBTableUtil.insertOrUpdate(conn, 
							dbTable.getTableName(), 
							dbTable.getPrimaryKeys(), primarykeyValueList, 
							dbTable.getCols(), otherColValueList);
					
				}
			} else {
				List<String> otherColValueList = new ArrayList<String>();
				otherColValueList.add(value);
				
				//insert or update
				updCnt += DBTableUtil.insertOrUpdate(conn, 
						dbTable.getTableName(), 
						dbTable.getPrimaryKeys(), primarykeyValueList, 
						dbTable.getCols(), otherColValueList);
			}
			
			return updCnt;
		} catch(Throwable e) {
			logger.error(null, e);
			return updCnt;
		} finally {
			try {
				conn.setAutoCommit(false);
			} catch(Throwable e) {
			}
			try {
				conn.close();
			} catch(Throwable e) {
			}
		}
		
	}
	
	private final ReentrantReadWriteLock _lockForAccessLockMapForDBCol = new ReentrantReadWriteLock();
	private final Map<String, ReentrantReadWriteLock> _lockMapForChangeDBCol = new ConcurrentHashMap<String, ReentrantReadWriteLock>();
	
	private DBCol checkColumnExists(Connection conn, DBTable dbTable, String colName, String colValue) throws SQLException {
		DBCol dbCol = null;
		dbCol = dbTable.getCol(colName); 
		if(dbCol != null) {
			return dbCol;
		}
		
		ReentrantReadWriteLock lockForChangeDBCol = acquireLockForChangeCol(
				dbTable.getTableName(), colName);
		lockForChangeDBCol.writeLock().lock();
		try {
			dbCol = dbTable.getCol(colName);
			if(dbCol == null) {
				//not exists
				logger.info("checkColumnExists() Column not exists." 
						+ " table:" + dbTable.getTableName() + ", colName:" + colName);
				
				dbCol = new DBCol(
						colName, 
						KeySchemaUtil.defaultDBColMaxLength(colValue.length()), 
						"varchar", "", true);
				DBTableUtil.alterTableAddColumn(conn, dbTable.getTableName(), dbCol);

				dbTable.addCol(dbCol);
				logger.info("checkColumnExists() Column Added. colName:" + colName); 
			}
		} finally {
			lockForChangeDBCol.writeLock().unlock();
		}

		return dbCol;
	}
	
	private void checkColumnMaxLength(Connection conn, DBTable dbTable, DBCol dbCol, String colValue) throws SQLException {
		int valLen = colValue.length();
		if(valLen > dbCol.getColMaxLength()) {
			
			ReentrantReadWriteLock lockForChangeDBCol = acquireLockForChangeCol(
					dbTable.getTableName(), dbCol.getColName());
			
			lockForChangeDBCol.writeLock().lock();
			try {
				if(valLen > dbCol.getColMaxLength()) {
					//change column length
					int newMaxLen = KeySchemaUtil.defaultDBColMaxLength(valLen);
					logger.info("checkColumnMaxLength() Column max length is going to be changed to " + newMaxLen + "." 
							+ " table:" + dbTable.getTableName() + ", colName:" + dbCol.getColName());
					
					DBCol newDBCol = new DBCol(dbCol.getColName(), newMaxLen, dbCol.getDataType(), dbCol.getExtra(), dbCol.isNullable());
					DBTableUtil.alterTableChangeColumn(conn, dbTable.getTableName(), newDBCol);

					dbCol.setColMaxLength(newMaxLen);

					logger.info("checkColumnMaxLength() Column max length is changed to " + newMaxLen); 
				}
			} finally {
				lockForChangeDBCol.writeLock().unlock();
			}
		}
	}
	
	private ReentrantReadWriteLock acquireLockForChangeCol(String tableName, String colName) {
		String keyForLock = keyOfLockForChangeDBColMap(tableName, colName);
		ReentrantReadWriteLock lockForChangeDBCol = null;
		
		_lockForAccessLockMapForDBCol.readLock().lock();
		try {
			lockForChangeDBCol = _lockMapForChangeDBCol.get(keyForLock);
			if(lockForChangeDBCol == null) {
				lockForChangeDBCol = new ReentrantReadWriteLock();
				_lockMapForChangeDBCol.put(keyForLock, lockForChangeDBCol);
			}
		} finally {
			_lockForAccessLockMapForDBCol.readLock().unlock();
		}
		
		return lockForChangeDBCol;
	}
	
	private static String keyOfLockForChangeDBColMap(String tableName, String colName) {
		return tableName.concat(".").concat(colName);
	}
}
