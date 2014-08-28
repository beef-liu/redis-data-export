package com.beef.redisexport.schema.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.pattern.LiteralPatternConverter;

import com.salama.util.StringUtil;
import com.salama.util.db.DBMetaUtil;
import com.salama.util.db.DBUtil;

public class DBTableUtil {
	private static String IdentifierQuoteString = null;
	
	//public final static String DEFAULT_DB_TYPE_PK = "char({0})";
	//public final static String DEFAULT_DB_TYPE_COL = "varchar({0})";
	
	
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

	public static String quoteSqlIdentifier(Connection conn, String sqlIdentifier) throws SQLException {
		if(IdentifierQuoteString == null) {
			DatabaseMetaData metaData = conn.getMetaData();
			IdentifierQuoteString = metaData.getIdentifierQuoteString();
		}
		
		return IdentifierQuoteString.concat(sqlIdentifier).concat(IdentifierQuoteString);
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
    		"SELECT COLUMN_NAME, COLUMN_DEFAULT, IS_NULLABLE, COLUMN_TYPE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, COLUMN_KEY, COLUMN_COMMENT, EXTRA " +
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
                col.setDataType(rs.getString("DATA_TYPE"));
                col.setExtra(rs.getString("EXTRA"));

                colKey = rs.getString("COLUMN_KEY");
                if (colKey.toUpperCase().equals("PRI"))
                {
                	table.addPrimaryKey(col);
                }
                else
                {
                	table.addCol(col);
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
	
	/**
	 * {0}:table name {1}:column definition
	 */
	private final static String SQL_ALTER_TABLE_ADD_COLUMN = "ALTER TABLE {0} ADD COLUMN {1}";
	public static void alterTableAddColumn(Connection conn, String tableName, 
			DBCol dbCol) throws SQLException {
		String sql = StringUtil.formatString(
				SQL_ALTER_TABLE_ADD_COLUMN,
				quoteSqlIdentifier(conn, tableName),
				makeSqlOfColumnDefinition(conn, 
						dbCol.getColName(), dbCol.getDataType(), dbCol.getColMaxLength(), dbCol.isNullable(), null, dbCol.getExtra())
				);
		
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
	
	/**
	 * {0}:table name {1}:old column name {2}:column definition
	 */
	private final static String SQL_ALTER_TABLE_CHANGE_COLUMN = "ALTER TABLE {0} CHANGE COLUMN {1} {2}";
	public static void alterTableChangeColumn(Connection conn, String tableName, 
			DBCol dbCol) throws SQLException {
		String sql = StringUtil.formatString(
				SQL_ALTER_TABLE_CHANGE_COLUMN,
				quoteSqlIdentifier(conn, tableName),
				dbCol.getColName(),
				makeSqlOfColumnDefinition(conn, 
						dbCol.getColName(), dbCol.getDataType(), dbCol.getColMaxLength(), dbCol.isNullable(), null, dbCol.getExtra())
				);
		
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

	private final static String SQL_PRIMARY_KEY = "PRIMARY KEY ({0})";
	public static void createTable(Connection conn, DBTable dbTable) throws SQLException {
		StringBuilder sql = new StringBuilder();
		
		sql.append("CREATE TABLE IF NOT EXISTS ")
			.append(quoteSqlIdentifier(conn, dbTable.getTableName()))
			.append(" (").append("\n");
		
		DBCol dbCol;
		StringBuilder pks = new StringBuilder();
		
		//primary keys
		for(int i = 0; i < dbTable.countOfPrimaryKey(); i++) {
			dbCol = dbTable.getPrimaryKey(i);
			
			sql.append("  ").append(makeSqlOfColumnDefinition(
					conn, dbCol.getColName(), dbCol.getDataType(), dbCol.getColMaxLength(), dbCol.isNullable(), null, dbCol.getExtra()))
					.append(",").append("\n");
			
			if(i != 0) {
				pks.append(",");
			}
			pks.append(quoteSqlIdentifier(conn, dbCol.getColName()));
		}
		
		//other cols
		for(int i = 0; i < dbTable.countOfCols(); i++) {
			dbCol = dbTable.getCol(i);
			
			sql.append("  ").append(makeSqlOfColumnDefinition(
					conn, dbCol.getColName(), dbCol.getDataType(), dbCol.getColMaxLength(), dbCol.isNullable(), null, dbCol.getExtra()))
					.append(",").append("\n");
		}
		
		//pk
		sql.append("  ").append(StringUtil.formatString(SQL_PRIMARY_KEY, pks.toString())).append("\n");
		
		//table comment
		sql.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8");
		if(dbTable.getComment() != null && dbTable.getComment().length() > 0) {
			sql.append(" COMMENT='").append(dbTable.getComment()).append("'");
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

	/**
	 * e.g., col1 char(32) null default null
	 * @param conn
	 * @param colName
	 * @param dataType
	 * @param maxLength
	 * @param nullable
	 * @param defaultValue
	 * @return
	 * @throws SQLException 
	 */
	private static String makeSqlOfColumnDefinition(
			Connection conn,
			String colName, String dataType, int maxLength,
			boolean nullable, String defaultValue, String extra
			) throws SQLException {
		StringBuilder sql = new StringBuilder();
		
		sql.append(" ").append(quoteSqlIdentifier(conn, colName));
		sql.append(" ").append(dataType);
		if(maxLength > 0) {
			sql.append("(").append(String.valueOf(maxLength)).append(")");
		}
		if(nullable) {
			sql.append(" null");
		} else {
			sql.append(" not null");
		}
		
		if(defaultValue != null && defaultValue.length() > 0) {
			sql.append(" default ").append("'").append(defaultValue).append("'");
		}
		
		if(extra != null && extra.length() > 0) {
			sql.append(" ").append(extra);
		}
		
		return sql.toString();
	}

	public static int insertOrUpdate(Connection conn,
			String tableName,
			List<DBCol> primaryKeyList, List<String> primarykeyValueList,
			List<DBCol> otherColList, List<String> otherColValueList) throws SQLException {
		try {
			return insert(conn, tableName, primaryKeyList, primarykeyValueList, otherColList, otherColValueList);
		} catch(SQLException e) {
			if(e.getClass().getSimpleName().equalsIgnoreCase("MySQLIntegrityConstraintViolationException")) {
				return update(conn, tableName, primaryKeyList, primarykeyValueList, otherColList, otherColValueList);
			} else {
				throw e;
			}			
		}
	}
	
	public static int insert(Connection conn,
			String tableName,
			List<DBCol> primaryKeyList, List<String> primarykeyValueList,
			List<DBCol> otherColList, List<String> otherColValueList) throws SQLException {
		//make sql ---------------------------
		StringBuilder sqlNames = new StringBuilder();
		StringBuilder sqlValues = new StringBuilder();
		
		DBCol col;
		for(int i = 0; i < primarykeyValueList.size(); i++) {
			col = primaryKeyList.get(i);
			
			if("auto_increment".equalsIgnoreCase(col.getExtra())) {
				continue;
			}
			
			if(sqlNames.length() > 0) {
				sqlNames.append(",");
			}
			sqlNames.append(quoteSqlIdentifier(conn, col.getColName()));
			
			if(sqlValues.length() > 0) {
				sqlValues.append(",");
			}
			sqlValues.append("?");
		}
		
		for(int i = 0; i < otherColList.size(); i++) {
			col = otherColList.get(i);
			
			if(sqlNames.length() > 0) {
				sqlNames.append(",");
			}
			sqlNames.append(quoteSqlIdentifier(conn, col.getColName()));
			
			if(sqlValues.length() > 0) {
				sqlValues.append(",");
			}
			sqlValues.append("?");
		}
		
		StringBuilder sql = new StringBuilder();
		
		sql.append("insert into ").append(quoteSqlIdentifier(conn, tableName)).append(" (");
		sql.append(sqlNames.toString());
		sql.append(") values (");
		sql.append(sqlValues.toString());
		sql.append(")");
		
		//execute ---------------------------
		PreparedStatement stmt = null;

		try {
			stmt = conn.prepareStatement(sql.toString());
			
			int index = 1;
			
			for(int i = 0; i < primarykeyValueList.size(); i++) {
				col = primaryKeyList.get(i);
				
				if("auto_increment".equalsIgnoreCase(col.getExtra())) {
					continue;
				}
				
				stmt.setString(index++, primarykeyValueList.get(i));
			}
			
			for(int i = 0; i < otherColList.size(); i++) {
				stmt.setString(index++, otherColValueList.get(i));
			}
			
			return stmt.executeUpdate();
		} finally {
			stmt.close();
		}
	}
	
	public static int update(Connection conn,
			String tableName,
			List<DBCol> primaryKeyList, List<String> primarykeyValueList,
			List<DBCol> otherColList, List<String> otherColValueList) throws SQLException {
		//make sql ---------------------------
		StringBuilder sqlSetValues = new StringBuilder();
		StringBuilder sqlWhereCondition = new StringBuilder();
		
		DBCol col;

		for(int i = 0; i < otherColList.size(); i++) {
			col = otherColList.get(i);
			
			if(sqlSetValues.length() > 0) {
				sqlSetValues.append(",");
			}
			sqlSetValues.append(" ").append(quoteSqlIdentifier(conn, col.getColName())).append(" = ?");
		}
		for(int i = 0; i < primarykeyValueList.size(); i++) {
			col = primaryKeyList.get(i);
			
			if(sqlWhereCondition.length() > 0) {
				sqlWhereCondition.append(" and");
			}
			sqlWhereCondition.append(" ").append(quoteSqlIdentifier(conn, col.getColName())).append(" = ?");
		}
		
		
		StringBuilder sql = new StringBuilder();
		
		sql.append("update ").append(quoteSqlIdentifier(conn, tableName)).append(" set ");
		sql.append(sqlSetValues.toString());
		sql.append(" where ");
		sql.append(sqlWhereCondition.toString());
		
		//execute ---------------------------
		PreparedStatement stmt = null;

		try {
			stmt = conn.prepareStatement(sql.toString());
			
			int index = 1;
			
			for(int i = 0; i < otherColList.size(); i++) {
				stmt.setString(index++, otherColValueList.get(i));
			}
			for(int i = 0; i < primarykeyValueList.size(); i++) {
				stmt.setString(index++, primarykeyValueList.get(i));
			}
			
			return stmt.executeUpdate();
		} finally {
			stmt.close();
		}
	}

}
