package com.beef.redisexport.schema.util;

public class DBCol {
	
	private String _colName;
	
	private String _dataType = "char";
	
	private int _colMaxLength = 255;
	
	private String _extra = "";
	
	private boolean _isNullable = true;

	public DBCol() {
		
	}
	
	public DBCol(String colName, int colMaxLength) {
		_colName = colName;
		_colMaxLength = colMaxLength;
	}

	public DBCol(String colName, int colMaxLength, String dataType, String extra) {
		_colName = colName;
		_colMaxLength = colMaxLength;
		_dataType = dataType;
		_extra = extra;
	}
	
	public DBCol(String colName, int colMaxLength, String dataType, String extra, boolean isNullable) {
		_colName = colName;
		_colMaxLength = colMaxLength;
		_dataType = dataType;
		_extra = extra;
		_isNullable = isNullable;
	}
	
	public String getColName() {
		return _colName;
	}

	public void setColName(String colName) {
		_colName = colName;
	}

	public int getColMaxLength() {
		return _colMaxLength;
	}

	public void setColMaxLength(int colMaxLength) {
		_colMaxLength = colMaxLength;
	}

	public String getDataType() {
		return _dataType;
	}

	public void setDataType(String dataType) {
		_dataType = dataType;
	}

	public String getExtra() {
		return _extra;
	}

	public void setExtra(String extra) {
		_extra = extra;
	}

	public boolean isNullable() {
		return _isNullable;
	}

	public void setNullable(boolean isNullable) {
		_isNullable = isNullable;
	}
	
}
