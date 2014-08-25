package com.beef.redisexport.schema.data;

import java.util.List;

public class KeyFieldDesc {
	/**
	 * Indicate DB Table column by ${colName}
	 * e.g., test.${ymd}.${userId}, and scan key match "test.*.*" in redis.
	 */
	private String _keyPattern;

	private List<FieldDesc> _fieldList = null;

	public String getKeyPattern() {
		return _keyPattern;
	}

	public void setKeyPattern(String keyPattern) {
		_keyPattern = keyPattern;
	}

	public List<FieldDesc> getFieldList() {
		return _fieldList;
	}

	public void setFieldList(List<FieldDesc> fieldList) {
		_fieldList = fieldList;
	}
	
}
