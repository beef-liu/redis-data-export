package com.beef.redisexport.schema.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DBTable {
    private String _tableName = "";
    
    private String _comment = "";

    private List<DBCol> _primaryKeys = new ArrayList<DBCol>();
    
    private Map<String, DBCol> _primarykeyMap = new HashMap<String, DBCol>();

	private List<DBCol> _cols = new ArrayList<DBCol>();

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
	
}
