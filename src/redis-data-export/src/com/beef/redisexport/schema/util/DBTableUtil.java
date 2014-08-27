package com.beef.redisexport.schema.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.salama.util.StringUtil;
import com.salama.util.db.DBMetaUtil;
import com.salama.util.db.DBUtil;

public class DBTableUtil {
	private static String IdentifierQuoteString = null;
	
	public final static String DEFAULT_DB_TYPE_PK = "char({0})";
	public final static String DEFAULT_DB_TYPE_COL = "varchar({0})";
	public final static String SQL_PRIMARY_KEY = "PRIMARY KEY ({0})";
	
	
	/*
	public static List<String> getAllTableName(Connection conn) throws SQLException {
		ResultSet rs = null;
		
		DatabaseMetaData meta = conn.getMetaData();
		
		rs = meta.getTables(conn.getCatalog(), null, null, new String[]{"TABLE"});
		
		List<String> tableNameList = new ArrayList<String>();
		
		while(rs.next()) {
			tableNameList.add(rs.getString("TABLE_NAME"));
		}

		return tableNameList;
	}
	*/
	
	public static boolean isTableExists(Connection conn, String tableName) throws SQLException {
		ResultSet rs = null;
		
		DatabaseMetaData meta = conn.getMetaData();

		rs = meta.getTables(conn.getCatalog(), null, tableName.toUpperCase(), new String[]{"TABLE"});
		
		if(rs.next()) {
			return true;
		} else {
			return false;
		}
	}

	public static String getIdentifierQuoteString(Connection conn) throws SQLException {
		if(IdentifierQuoteString == null) {
			DatabaseMetaData metaData = conn.getMetaData();
			IdentifierQuoteString = metaData.getIdentifierQuoteString();
		}
		
		return IdentifierQuoteString;
	}

	private final static String SQL_SHOW_TABLES = "show tables";
    public static List<String> getAllTables(Connection conn) throws SQLException {
    	List<String> tableNameList = new ArrayList<String>();
    	Statement stmt = null;
    	ResultSet rs = null;
    	
    	try {
        	stmt = conn.createStatement();
        	rs = stmt.executeQuery(SQL_SHOW_TABLES);
        	
        	while(rs.next()) {
        		tableNameList.add(rs.getString(1));
        	}

        	return tableNameList;
    	} finally {
    		try {
        		stmt.close();
    		} catch(SQLException e) {
    		}
    	}
    }
	
    private final static String SQL_GET_TABLE_COMMENT = 
    		"SELECT TABLE_COMMENT " +
    		"FROM information_schema.tables " +
    		"WHERE upper(table_name) = upper('{0}')";
    private final static String SQL_GET_COLUMNS_INFO = 
    		"SELECT COLUMN_NAME, COLUMN_DEFAULT, IS_NULLABLE, COLUMN_TYPE, CHARACTER_MAXIMUM_LENGTH, COLUMN_KEY, COLUMN_COMMENT, EXTRA " +
    		"FROM information_schema.columns " +
    		"WHERE upper(table_name) = upper('{0}') order by ORDINAL_POSITION";
	public static DBTable getDBTable(Connection conn, String tableName) throws SQLException {
		DBTable table = null;
    	String sql = "";
        Statement stmt = null;
    	ResultSet rs = null;
        
    	//Get comment
    	try {
            sql = StringUtil.formatString(SQL_GET_TABLE_COMMENT, tableName);
        	stmt = conn.createStatement();
        	rs = stmt.executeQuery(sql);
        	
        	if(rs.next()) {
                table = new DBTable();
                table.setTableName(tableName);
        		table.setComment(rs.getString("TABLE_COMMENT"));
        	} else {
        		return null;
        	}
    	} finally {
    		try {
        		stmt.close();
    		} catch(SQLException e) {
    		}
    	}

    	//Get column info
    	try {
            sql = StringUtil.formatString(SQL_GET_COLUMNS_INFO, tableName);
        	stmt = conn.createStatement();
        	rs = stmt.executeQuery(sql);
        	
        	//Object defaultValue = null;
        	//String isNullable = "";
        	String colKey = "";

        	while(rs.next()) {
                DBCol col = new DBCol();

                col.setColName(rs.getString("COLUMN_NAME"));
                col.setColMaxLength(rs.getInt("CHARACTER_MAXIMUM_LENGTH"));

                colKey = rs.getString("COLUMN_KEY");
                if (colKey.toUpperCase().equals("PRI"))
                {
                	table.getPrimaryKeys().add(col);
                	table.getPrimarykeyMap().put(col.getColName(), col);
                }
                else
                {
                	table.getCols().add(col);
                }
        	}
        	
        	return table;
    	} finally {
    		try {
        		stmt.close();
    		} catch(SQLException e) {
    		}
    	}
	}
	
	private final static String ALTER_TABLE_COL_MAX_LENGTH = "ALTER TABLE ${0} CHANGE COLUMN ${1} ${2} ${3}(${4})";
	public static void alterTableChangeColumn(Connection conn, String tableName, 
			String oldColName, String colName, String colType, int colMaxLength, boolean isNullable) throws SQLException {
		String identifierQuote = getIdentifierQuoteString(conn);
		
		StringBuilder sql = new StringBuilder();
		sql.append(StringUtil.formatString(
						ALTER_TABLE_COL_MAX_LENGTH, 
						identifierQuote.concat(tableName).concat(identifierQuote),
						identifierQuote.concat(oldColName).concat(identifierQuote),
						identifierQuote.concat(colName).concat(identifierQuote),
						colType,
						String.valueOf(colMaxLength)
						)
				);
		
		if(isNullable) {
			sql.append(" NULL DEFAULT NULL");
		} else {
			sql.append(" NOT NULL");
		}
		
		//execute sql
		PreparedStatement stmt = null;
		
		try {
			stmt = conn.prepareStatement(sql.toString());
			
			stmt.executeUpdate();
    	} finally {
    		try {
        		stmt.close();
    		} catch(SQLException e) {
    		}
		}
	}
	
	public static void createTable(Connection conn, DBTable dbTable) throws SQLException {
		StringBuilder sql = new StringBuilder();
		
		String identifierQuote = getIdentifierQuoteString(conn);
		
		sql.append("CREATE TABLE ").append(identifierQuote).append(dbTable.getTableName()).append(identifierQuote).append(" (").append("\n");
		
		DBCol dbCol;
		StringBuilder pks = new StringBuilder();
		
		//primary keys
		for(int i = 0; i < dbTable.getPrimaryKeys().size(); i++) {
			dbCol = dbTable.getPrimaryKeys().get(i);
			
			sql.append("  ").append(identifierQuote.concat(dbCol.getColName()).concat(identifierQuote))
			.append(" ").append(StringUtil.formatString(DEFAULT_DB_TYPE_PK, Integer.toString(dbCol.getColMaxLength())))
			.append(" NOT NULL ").append(",").append("\n");
			
			if(i != 0) {
				pks.append(",");
			}
			pks.append(identifierQuote.concat(dbCol.getColName()).concat(identifierQuote));
		}
		
		//other cols
		for(int i = 0; i < dbTable.getCols().size(); i++) {
			dbCol = dbTable.getCols().get(i);
			
			sql.append("  ").append(identifierQuote.concat(dbCol.getColName()).concat(identifierQuote))
			.append(" ").append(StringUtil.formatString(DEFAULT_DB_TYPE_COL, Integer.toString(dbCol.getColMaxLength())))
			.append(" DEFAULT NULL ").append(",").append("\n");
		}
		
		//pk
		sql.append("  ").append(StringUtil.formatString(SQL_PRIMARY_KEY, pks.toString())).append("\n");
		//table comment
		sql.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8 ");
		if(dbTable.getComment() != null && dbTable.getComment().length() > 0) {
			sql.append("COMMENT='").append(dbTable.getComment()).append("'");
		}
		
		//execute sql
		PreparedStatement stmt = null;
		
		try {
			stmt = conn.prepareStatement(sql.toString());
			
			stmt.executeUpdate();
    	} finally {
    		try {
        		stmt.close();
    		} catch(SQLException e) {
    		}
		}
	} 
	
}
