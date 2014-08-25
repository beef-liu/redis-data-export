package com.beef.redisexport.schema.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DBTable {
    private String _tableName = "";
    
    private String _comment = "";

    private List<String> _primaryKeys = new ArrayList<String>();
    
    private Set<String> _primarykeySet = new HashSet<String>();

	private List<String> _cols = new ArrayList<String>();

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

	public List<String> getPrimaryKeys() {
		return _primaryKeys;
	}

	public void setPrimaryKeys(List<String> primaryKeys) {
		_primaryKeys = primaryKeys;
	}

	/**
	 * Not include primary keys
	 * @return
	 */
	public List<String> getCols() {
		return _cols;
	}

	public void setCols(List<String> cols) {
		_cols = cols;
	}
    
    
    public Set<String> getPrimarykeySet() {
		return _primarykeySet;
	}

	public void setPrimarykeySet(Set<String> primarykeySet) {
		_primarykeySet = primarykeySet;
	}
}
