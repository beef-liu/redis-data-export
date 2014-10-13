package com.beef.redisexport.util;

import java.sql.Connection;
import java.sql.SQLException;

public interface IDBPool {
	
	public Connection getConnection() throws SQLException;

}
