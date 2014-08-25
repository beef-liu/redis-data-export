package com.beef.redisexport.schema.util;

import java.util.ArrayList;
import java.util.StringTokenizer;

import com.beef.redisexport.schema.data.FieldDesc;
import com.beef.redisexport.schema.data.KeyDesc;
import com.beef.redisexport.schema.data.KeyFieldDesc;
import com.beef.redisexport.schema.data.ValueDesc;

public class KeySchemaUtil {
	/**
	 * 
	 * @param keyPattern could be test.${userId}.a
	 * @return
	 */
	public static KeyPattern parseKeyPattern(String keyPattern) {
		KeyPattern pattern = new KeyPattern();

		StringBuilder sb = new StringBuilder();
		int beginIndex = 0;
		int index = 0;
		int index2 = 0;
		while(beginIndex <= keyPattern.length()) {
			index = keyPattern.indexOf("${", beginIndex);
			
			if(index < 0) {
				sb.append(keyPattern.substring(beginIndex));
				break;
			}
			
			index2 = keyPattern.indexOf('}', index);
			if(index2 < 0) {
				throw new RuntimeException("Wrong keyPattern:" + keyPattern);
			}
			
			//substring before ${
			sb.append(keyPattern.substring(beginIndex, index));

			//append *
			sb.append('*');
			
			//substring between ${ and }, (variate key name)
			if(pattern.getVariateKeyNames() == null) {
				pattern.setVariateKeyNames(new ArrayList<String>());
			}
			pattern.getVariateKeyNames().add(keyPattern.substring(index + 2, index2).trim());
			
			//set beginIndex after }
			beginIndex = index2 + 1;
		}

		pattern.setKeyMatchPattern(sb.toString());
		
		return pattern;
	}

	/**
	 * test.*.a -> test_a
	 * @param keyPattern
	 * @param key
	 * @param keyDesc
	 * @return Not include nonprimarykey columns.
	 */
	public static DBTable parseDBTable(KeyPattern keyPattern, String key, ValueDesc valDesc) {
		DBTable table = new DBTable();
		
		String tableName = parseTableName(keyPattern, valDesc);
		table.setTableName(tableName);
		table.setComment("from redis:" + keyPattern.getKeyMatchPattern());
		
		String pk;
		for(int i = 0; i < keyPattern.getVariateKeyNames().size(); i++) {
			pk = keyPattern.getVariateKeyNames().get(i);
			table.getPrimaryKeys().add(pk);
			table.getPrimarykeySet().add(pk);
		}
		
		if(valDesc != null) {
			String[] pks = parsePrimaryKeys(valDesc);
			
			if(pks != null) {
				for(int i = 0; i < pks.length; i++) {
					table.getPrimaryKeys().add(pks[i]);
					table.getPrimarykeySet().add(pks[i]);
				}
			}
		}
		
		return table;
	}
	
	private static String[] parsePrimaryKeys(ValueDesc valDesc) {
		if(valDesc.getPrimaryKeysInData() != null && valDesc.getPrimaryKeysInData().length() > 0) {
			StringTokenizer stk = new StringTokenizer(valDesc.getPrimaryKeysInData(), ",");
			
			String[] pks = new String[stk.countTokens()];
			int i = 0;
			while(stk.hasMoreTokens()) {
				pks[i++] = stk.nextToken();
			}
			
			return pks;
		} else {
			return null;
		}
	}
	
	/**
	 * 
	 * @param keyPattern
	 * @param valDesc
	 * @return lowercase
	 */
	public static String parseTableName(KeyPattern keyPattern, ValueDesc valDesc) {
		StringBuilder sb = new StringBuilder();
		
		char curC;
		for(int i = 0; i < keyPattern.getKeyMatchPattern().length(); i++) {
			curC = keyPattern.getKeyMatchPattern().charAt(i);
			
			if(curC == '*') {
				continue;
			}
			
			if(curC == '.' || curC == '-') {
				if(sb.charAt(sb.length() - 1) != '_') {
					sb.append('_');
				}
			} else {
				sb.append(curC);
			}
		}

		if(valDesc != null && FieldDesc.class.isAssignableFrom(valDesc.getClass())) {
			if(sb.charAt(sb.length() - 1) != '_') {
				sb.append('_');
			}
			sb.append(((FieldDesc)valDesc).getFieldName());
		}
		
		return sb.toString().toLowerCase();
	}
	
}
