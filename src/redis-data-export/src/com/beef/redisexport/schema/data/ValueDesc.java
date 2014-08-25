package com.beef.redisexport.schema.data;

public class ValueDesc {
	/**
	 * true: value is in xml format of data. Children nodes in xml are DB Column.
	 */
	private boolean _isDataXml = false;
	
	/**
	 * only be used when _isDataXml is true. Array of name of primary key, e.g. pkName1,pkName2,pkName3
	 */
	private String _primaryKeysInData = "";
	
	/**
	 * empty: not compressed lzf: compressed in lzf  gzip:compressed in gzip
	 */
	private String _compressMode = "";

	public boolean isDataXml() {
		return _isDataXml;
	}

	public void setDataXml(boolean isDataXml) {
		_isDataXml = isDataXml;
	}

	public String getCompressMode() {
		return _compressMode;
	}

	public void setCompressMode(String compressMode) {
		_compressMode = compressMode;
	}

	public String getPrimaryKeysInData() {
		return _primaryKeysInData;
	}

	public void setPrimaryKeysInData(String primaryKeysInData) {
		_primaryKeysInData = primaryKeysInData;
	}
	
}
