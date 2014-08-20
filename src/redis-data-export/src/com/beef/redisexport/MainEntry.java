package com.beef.redisexport;

import org.apache.log4j.Logger;

import com.beef.redisexport.handler.DefaultRedisDataExportHandler;
import com.beef.redisexport.interfaces.IRedisDataHandler;

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
				RedisDataExportContext.singleton().reload();
				
				//start data iterator
				startDataIterator(args);
			}
			
		} catch(Throwable e) {
			logger.error(null, e);
		} finally {
			RedisDataExportContext.singleton().destroy();
			System.out.println(":-)Main Thread end -----------------------------------");
		}
	}
	
	private static void outputHelpContent() {
		
		System.out.println();
		System.out.println(":-)help ---------------------------------------------------------------");
		System.out.println("--help");
		System.out.println("-p keyPattern -h dataHandlerName -t threadCount -s scanCount");
		System.out.println();
		System.out.println("-p (optional, default empty): pattern to scan keys. e.g., *test*. Please see the doc of scan command of redis.");
		System.out.println("-h (optional, default " + DefaultRedisDataExportHandler.class.getName() + "): the full class name (e.g., com.test.TestDataHandler) of the data handler which implements IRedisDataHandler.");
		System.out.println("-t (optional, default 5): the count of thread running concurrently");
		System.out.println("-s (optional, default 100): the count of scanning keys at one time in each thread");
		
		System.out.println();
		System.out.println(":-)log file is supposed at ./redis-data-export.log --------------------");
		System.out.println();
	}

	private static void startDataIterator(String[] args) {
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
		int threadCount = 5;
		int scanCount = 100;
		String redisDataHandlerName = DefaultRedisDataExportHandler.class.getName();
		
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
		
		System.out.println("Params: :-) --------------------------------");
		System.out.println("  keyPattern:" + keyPattern);
		System.out.println("  redisDataHandlerName:" + redisDataHandlerName);
		System.out.println("  threadCount:" + threadCount);
		System.out.println("  scanCount:" + scanCount);
		System.out.println("--------------------------------------------");

		IRedisDataHandler redisDataHandler;
		
		try {
			Class<?> redisDataHandlerClass = Class.forName(redisDataHandlerName);
			redisDataHandler = (IRedisDataHandler) redisDataHandlerClass.newInstance();
		} catch(Throwable e) {
			logger.error(null, e);
			System.out.println("Initialize data handler failed (" + redisDataHandlerName + ") :-! ------------------------");
			return;
		}
		
		RedisDataIterator dataIterator = new RedisDataIterator(keyPattern, threadCount, scanCount, redisDataHandler);
		
		dataIterator.waitForever();
	}
}
