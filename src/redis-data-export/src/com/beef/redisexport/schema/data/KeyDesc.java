package com.beef.redisexport.schema.data;

public class KeyDesc {
	/**
	 * Indicate DB Table column by ${colName}
	 * e.g., test.${ymd}.${userId}, and scan key match "test.*.*" in redis.
	 */
	private String _keyPattern = null;

	/**
	 * be null when key type is not hash
	 */
	private String _fieldName = null;

	private ValueDesc _valDesc = null;

	public String getKeyPattern() {
		return _keyPattern;
	}

	public void setKeyPattern(String keyPattern) {
		_keyPattern = keyPattern;
	}

	public String getFieldName() {
		return _fieldName;
	}

	public void setFieldName(String fieldName) {
		_fieldName = fieldName;
	}
	
	public ValueDesc getValDesc() {
		return _valDesc;
	}

	public void setValDesc(ValueDesc valDesc) {
		_valDesc = valDesc;
	}
	
	
}
