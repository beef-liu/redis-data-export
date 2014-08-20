package com.beef.redisexport;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import org.apache.log4j.Logger;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import MetoXML.XmlDeserializer;
import MetoXML.Base.XmlParseException;

import com.beef.redisexport.config.DBConfig;
import com.beef.redisexport.config.RedisConfig;
import com.beef.redisexport.util.DBPool;
import com.salama.reflect.PreScanClassFinder;

public class RedisDataExportContext {
	private final static Logger logger = Logger.getLogger(RedisDataExportContext.class);
	
	public static final String DIR_NAME_CONF = "conf";
	public static final String FILE_NAME_DB_CONFIG = "DBConfig.xml";
	public static final String FILE_NAME_REDIS_CONFIG = "RedisConfig.xml";
	
	private static RedisDataExportContext _singleton = null;
	
	private File _workDir;
	private DBPool _dbPool;
	private JedisPool _jedisPool;

	private PreScanClassFinder _preScanClassFinder = null;
	
	public DBPool getDbPool() {
		return _dbPool;
	}

	public JedisPool getJedisPool() {
		return _jedisPool;
	}

	private RedisDataExportContext() {
		File workDir =  new File("");
		System.out.println("Run at " + workDir.getAbsolutePath());
		
		_workDir = new File(workDir.getAbsolutePath()); 
	}
	
	public static RedisDataExportContext singleton() {
		if(_singleton == null) {
			_singleton = new RedisDataExportContext();
		}
		
		return _singleton;
	}

	protected void reload() throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException {
		System.out.println("RedisDataExportContext reload() -----------------");
		
		File configDir =  new File(_workDir, DIR_NAME_CONF);
		logger.info("configDir:" + configDir.getAbsolutePath());
		System.out.println("configDir:" + configDir.getAbsolutePath());
		
		//init classFinder
		/*
		{
			if(_preScanClassFinder == null) {
				_preScanClassFinder = new PreScanClassFinder();
			} else {
				_preScanClassFinder.clearPreScannedClass();
			}
			
			_preScanClassFinder.loadClassOfPackage("com.beef.redisexport.handler");
		}
		*/
		
		//init DBPool
		{
			File configFile = new File(configDir, FILE_NAME_DB_CONFIG);
			
			XmlDeserializer xmlDes = new XmlDeserializer();
			DBConfig config = (DBConfig) xmlDes.Deserialize(
					configFile.getAbsolutePath(), 
					DBConfig.class, XmlDeserializer.DefaultCharset);
			_dbPool = new DBPool(config);
			logger.info("dbPool:" + config.getUrl() 
					+ " user:" + config.getUserName()
					+ " maxActive:" + config.getMaxActive()
					+ " maxIdle:" + config.getMaxIdle()
					+ " maxWait:" + config.getMaxWait()
					+ " autoCommit:" + (config.getDefaultAutoCommit() == 0? "false" : "true")
					);
		}
		
		//init jedis pool
		{
			File configFile = new File(configDir, FILE_NAME_REDIS_CONFIG);
			
			XmlDeserializer xmlDes = new XmlDeserializer();
			RedisConfig config = (RedisConfig) xmlDes.Deserialize(
					configFile.getAbsolutePath(), 
					RedisConfig.class, XmlDeserializer.DefaultCharset);

			JedisPoolConfig jedisConfig = new JedisPoolConfig();
			jedisConfig.setMaxIdle(config.getMaxIdle());
			/* old jedis version
			jedisConfig.setMaxActive(_jedisPoolMaxActive);
			jedisConfig.setMaxWait(_jedisPoolMaxWait);
			*/
			jedisConfig.setMaxTotal(config.getMaxTotal());
			jedisConfig.setMaxWaitMillis(config.getMaxWaitMillis());
			
			jedisConfig.setSoftMinEvictableIdleTimeMillis(config.getSoftMinEvictableIdleTimeMillis());
			jedisConfig.setTestOnBorrow(config.isTestOnBorrow());
			
			_jedisPool = new JedisPool(jedisConfig, config.getHost(), config.getPort()); 
			logger.info("jedisPool:" + config.getHost() + ":" + config.getPort()
					+ " maxTotal:" + jedisConfig.getMaxTotal() + " maxIdle:" + jedisConfig.getMaxIdle()
					+ " maxWaitMillis:" + jedisConfig.getMaxWaitMillis() 
					+ " softMinEvictableIdleTimeMillis:" + jedisConfig.getSoftMinEvictableIdleTimeMillis()
					+ " testOnBorrow:" + jedisConfig.getTestOnBorrow() 
					);
		}
		
	}
	
	public void destroy() {
		if(_jedisPool != null) {
			System.out.println("RedisDataExportContext destroy() -----------------");
			
			_jedisPool.destroy();
			_jedisPool = null;
		}
	}
	
	public final File getWorkDir() {
		return _workDir;
	}
	
}
