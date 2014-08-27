package com.beef.redisexport.schema.util;

public class DBCol {
	
	private String _colName;
	
	private int _colMaxLength = 255;

	public DBCol() {
		
	}
	
	public DBCol(String colName, int colMaxLength) {
		_colName = colName;
		_colMaxLength = colMaxLength;
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

	
}
