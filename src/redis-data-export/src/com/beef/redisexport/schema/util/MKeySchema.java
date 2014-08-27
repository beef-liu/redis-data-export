package com.beef.redisexport.schema.util;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.beef.redisexport.schema.data.KeyDesc;
import com.beef.redisexport.schema.data.KeySchema;

public class MKeySchema {
	/**
	 * key:keyPattern value: KeyDesc
	 */
	private Map<String, KeyDesc> _keyDescMap = new ConcurrentHashMap<String, KeyDesc>();

	public KeyDesc getKeyDesc(String keyPattern, String fieldName) {
		return _keyDescMap.get(getMapKey(keyPattern, fieldName));
	}
	
	/**
	 * 
	 * @param keyPattern
	 * @param fieldName could be null
	 * @param valueDesc
	 */
	public void setKeyDesc(String keyPattern, String fieldName, KeyDesc keyDesc) {
		_keyDescMap.put(getMapKey(keyPattern, fieldName), keyDesc);
	}
	
	public static MKeySchema convertKeySchema(KeySchema keySchema) {
		MKeySchema mKeySchema = new MKeySchema();
		
		if(keySchema.getKeyDescs() != null) {
			KeyDesc keyDesc;
			for(int i = 0; i < keySchema.getKeyDescs().size(); i++) {
				keyDesc = keySchema.getKeyDescs().get(i);
				mKeySchema.setKeyDesc(keyDesc.getKeyPattern(), keyDesc.getFieldName(), keyDesc);
			}
		}
		
		return mKeySchema;
	}

	private static String getMapKey(String keyPattern, String fieldName) {
		if(fieldName == null || fieldName.length() > 0) {
			return keyPattern;
		} else {
			return fieldName.concat(".").concat(fieldName);
		}
	}
	
}
