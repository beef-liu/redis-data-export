package com.beef.redisexport.schema.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DBTableUtil {
	
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
	
}
