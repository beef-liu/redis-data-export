package com.beef.redisexport.schema.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DBTable {
    private String _tableName = "";
    
    private String _comment = "";

    private List<DBCol> _primaryKeys = new ArrayList<DBCol>();
    private Map<String, DBCol> _primarykeyMap = new ConcurrentHashMap<String, DBCol>();

	private List<DBCol> _cols = new ArrayList<DBCol>();
	private Map<String, DBCol> _colMap = new ConcurrentHashMap<String, DBCol>();

	public String getTableName() {
		return _tableName;
	}

	public void setTableName(String tableName) {
		_tableName = tableName;
	}

	public String getComment() {
		return _comment;
	}

	public void setComment(String comment) {
		_comment = comment;
	}

	public int countOfPrimaryKey() {
		return _primaryKeys.size();
	}
	
	public void addPrimaryKey(DBCol dbCol) {
		DBCol oldCol = _primarykeyMap.put(dbCol.getColName(), dbCol);
		if(oldCol == null) {
			_primaryKeys.add(dbCol);
		}
	}
	
	public DBCol getPrimaryKey(int index) {
		return _primaryKeys.get(index);
	}
	
	public DBCol getPrimaryKey(String colName) {
		return _primarykeyMap.get(colName);
	}

	public int countOfCols() {
		return _cols.size();
	}
	
	public void addCol(DBCol dbCol) {
		DBCol oldCol = _colMap.put(dbCol.getColName(), dbCol);
		if(oldCol == null) {
			_cols.add(dbCol);
		}
	}
	
	public DBCol getCol(int index) {
		return _cols.get(index);
	}
	
	public DBCol getCol(String colName) {
		return _colMap.get(colName);
	}
	
	/*
	public List<DBCol> getPrimaryKeys() {
		return _primaryKeys;
	}

	public void setPrimaryKeys(List<DBCol> primaryKeys) {
		_primaryKeys = primaryKeys;
	}

	public Map<String, DBCol> getPrimarykeyMap() {
		return _primarykeyMap;
	}

	public List<DBCol> getCols() {
		return _cols;
	}

	public void setCols(List<DBCol> cols) {
		_cols = cols;
	}
	*/
	
}
