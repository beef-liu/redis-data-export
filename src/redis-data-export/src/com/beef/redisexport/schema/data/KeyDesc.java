package com.beef.redisexport.schema.data;

public class KeyDesc {
	/**
	 * Indicate DB Table column by ${colName}
	 * e.g., test.${ymd}.${userId}, and scan key match "test.*.*" in redis.
	 */
	private String _keyPattern;
	
	private ValueDesc _valDesc = null;

	public String getKeyPattern() {
		return _keyPattern;
	}

	public void setKeyPattern(String keyPattern) {
		_keyPattern = keyPattern;
	}

	public ValueDesc getValDesc() {
		return _valDesc;
	}

	public void setValDesc(ValueDesc valDesc) {
		_valDesc = valDesc;
	}
	
	
}
