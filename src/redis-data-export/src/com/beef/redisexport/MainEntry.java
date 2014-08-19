package com.beef.redisexport;

public class MainEntry {

	public static void main(String[] args) {
		try {
			
			
		} finally {
			RedisDataExportContext.singleton().destroy();
		}
	}
	
}
