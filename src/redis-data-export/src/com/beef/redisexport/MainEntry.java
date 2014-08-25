package com.beef.redisexport;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;

import MetoXML.XmlDeserializer;
import MetoXML.Base.XmlParseException;
import MetoXML.Util.ClassFinder;

import com.beef.redisexport.config.DBConfig;
import com.beef.redisexport.config.RedisConfig;
import com.beef.redisexport.handler.DefaultRedisDataExportHandler;
import com.beef.redisexport.interfaces.IRedisDataHandler;
import com.beef.redisexport.schema.data.KeySchema;
import com.salama.reflect.PreScanClassFinder;

public class MainEntry {
	private final static Logger logger = Logger.getLogger(MainEntry.class);
	
	public final static String ARG0_HELP = "--help";
	
	/**
	 * 
	 * @param args
	 * --help: display the help tutorial
	 * -p ${keyPattern} -h ${handler which implements IRedisDataHandler} -t ${threadCount} -s ${scanCount}
	 */
	public static void main(String[] args) {
		RedisDataExportContext redisDataExportContext = null;
		try {
			String args0 = "";
			if(args.length > 0) {
				System.out.println("args[0]:" + args[0]);
				args0 = args[0];
			}
			
			if(args0.equals(ARG0_HELP)) {
				outputHelpContent();
			} else {
				//init context
				redisDataExportContext = new RedisDataExportContext();
				redisDataExportContext.reload();
				
				//start data iterator
				startDataIterator(args, redisDataExportContext);
			}
			
		} catch(Throwable e) {
			logger.error(null, e);
		} finally {
			if(redisDataExportContext != null) {
				redisDataExportContext.destroy();
			}
			System.out.println(":-)Main Thread end -----------------------------------");
		}
	}
	
	private static void outputHelpContent() {
		
		System.out.println();
		System.out.println(":-)help ---------------------------------------------------------------");
		System.out.println("--help");
		System.out.println("-p keyPattern -h dataHandlerName -t threadCount -s scanCount");
		System.out.println("-d schemaFile -h dataHandlerName -t threadCount -s scanCount");
		System.out.println("-a keyPatternArray -h dataHandlerName -t threadCount -s scanCount");
		System.out.println();
		System.out.println("-p (optional, default empty): pattern to scan keys. e.g., *test*. Please see the doc of scan command of redis.");
		System.out.println("-d (optional): The file path of schema description xml of KeySchema.");
		System.out.println("-a (optional): keyPattern array(separated by comma, e.g., test.${userId},test.${shopId})");
		System.out.println("-h (optional, default " + DefaultRedisDataExportHandler.class.getName() + "): the full class name (e.g., com.test.TestDataHandler) of the data handler which implements IRedisDataHandler.");
		System.out.println("-t (optional, default 5): the count of thread running concurrently");
		System.out.println("-s (optional, default 100): the count of scanning keys at one time in each thread");
		
		System.out.println();
		System.out.println(":-)log file is supposed at ./redis-data-export.log --------------------");
		System.out.println();
	}

	private static void startDataIterator(String[] args, RedisDataExportContext redisDataExportContext) throws XmlParseException, IOException, InvocationTargetException, IllegalAccessException, InstantiationException, NoSuchMethodException {
		if((args.length % 2) != 0) {
			System.out.println("args is wrong :-! ------------------------");
			outputHelpContent();
			return;
		}
		
		if(args.length == 0) {
			outputHelpContent();
		}
		
		//default value
		String keyPattern = null;
		String schemaPath = null;
		String keyPatternArray = null;
		int threadCount = 5;
		int scanCount = 100;
		String redisDataHandlerName = DefaultRedisDataExportHandler.class.getName();

		int keyParamCount = 0;
		
		try {
			int i = 0;
			String paramType;
			while(i < args.length) {
				if(args[i].charAt(0) == '-') {
					paramType = args[i];
					
					i++;
					if(args[i].charAt(0) == '-') {
						continue;
					} else {
						if(paramType.equals("-p")) {
							keyPattern = args[i];
							keyParamCount++;
						} else if(paramType.equals("-d")) {
							schemaPath = args[i];
							keyParamCount++;
						} else if(paramType.equals("-a")) {
							keyPatternArray = args[i];
							keyParamCount++;
						} else if(paramType.equals("-h")) {
							redisDataHandlerName = args[i];
						} else if(paramType.equals("-t")) {
							threadCount = Integer.parseInt(args[i]);
						} else if(paramType.equals("-s")) {
							scanCount = Integer.parseInt(args[i]);
						} else {
							System.out.println("Unknown args:" + paramType);
						}
					}
				}
				
				i++;
			}
		} catch(Throwable e) {
			logger.error(null, e);
			System.out.println("args is wrong :-! ------------------------");
			outputHelpContent();
			return;
		}
		if(keyParamCount > 1) {
			throw new RuntimeException("-p -d -a, only one of them can be assigned");
		}
		
		System.out.println("Params: :-) --------------------------------");
		if(keyPattern != null) {
			System.out.println("  keyPattern:" + keyPattern);
		} else if (schemaPath != null) {
			System.out.println("  schemaPath:" + schemaPath);
		} else if (keyPatternArray != null) {
			System.out.println("  keyPatternArray:" + keyPatternArray);
		}
		System.out.println("  redisDataHandlerName:" + redisDataHandlerName);
		System.out.println("  threadCount:" + threadCount);
		System.out.println("  scanCount:" + scanCount);
		System.out.println("--------------------------------------------");

		//Default ClassFinder ----------------------
		PreScanClassFinder defaultClassFinder = new PreScanClassFinder();
		defaultClassFinder.loadClassOfPackage(DefaultRedisDataExportHandler.class.getPackage().getName());		
		//defaultClassFinder.loadClassOfPackage(DBConfig.class.getPackage().getName());		
		//defaultClassFinder.loadClassOfPackage(KeySchema.class.getPackage().getName());		
		
		//schema
		KeySchema keySchema = null;
		if(schemaPath != null) {
			File schemaFile = new File(schemaPath);
			XmlDeserializer xmlDes = new XmlDeserializer();
			keySchema = (KeySchema) xmlDes.Deserialize(
					schemaFile.getAbsolutePath(), KeySchema.class, 
					XmlDeserializer.DefaultCharset, defaultClassFinder);
		} else if (keyPatternArray != null) {
			//auto generate keySchema
		}
		
		//Data Handler -----------------------------
		IRedisDataHandler redisDataHandler;
		
		try {
			Class<?> redisDataHandlerClass = Class.forName(redisDataHandlerName);
			redisDataHandler = (IRedisDataHandler) redisDataHandlerClass.newInstance();
			redisDataHandler.init(redisDataExportContext.getJedisPool(), redisDataExportContext.getDbPool(), keySchema);
		} catch(Throwable e) {
			logger.error(null, e);
			System.out.println("Initialize data handler failed (" + redisDataHandlerName + ") :-! ------------------------");
			return;
		}
		
		ArrayList<String> keyPatternList = new ArrayList<String>();
		if(schemaPath == null) {
			keyPatternList.add(keyPattern);
		} else {
			if(keySchema.getKeyDescs() != null) {
				for(int i = 0; i < keySchema.getKeyDescs().size(); i++) {
					keyPatternList.add(keySchema.getKeyDescs().get(i).getKeyPattern());
				}
			}
			if(keySchema.getKeyFieldDescs() != null) {
				for(int i = 0; i < keySchema.getKeyFieldDescs().size(); i++) {
					keyPatternList.add(keySchema.getKeyFieldDescs().get(i).getKeyPattern());
				}
			}
		}
		
		//start execute 
		for(int i = 0; i < keyPatternList.size(); i++) {
			logger.info("iterate keyPattern:" + keyPatternList.get(i) + " ----------------------------------");
			System.out.println("iterate keyPattern:" + keyPatternList.get(i) + " ----------------------------------");
			
			RedisDataIterator dataIterator = new RedisDataIterator(
					redisDataExportContext.getJedisPool(),
					keyPatternList.get(i), 
					threadCount, scanCount, redisDataHandler);
			dataIterator.waitForever();
		}
		
	}
	
}
