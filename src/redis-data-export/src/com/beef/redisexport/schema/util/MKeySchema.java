package com.beef.redisexport.schema.util;

import java.util.HashMap;

import com.beef.redisexport.schema.data.KeyDesc;
import com.beef.redisexport.schema.data.KeySchema;
import com.beef.redisexport.schema.data.ValueDesc;

public class MKeySchema {
	/**
	 * key:keyPattern value: ValueDesc
	 */
	private HashMap<String, ValueDesc> valueDescMap = new HashMap<String, ValueDesc>();

	public ValueDesc getValueDesc(String keyPattern, String fieldName) {
		return valueDescMap.get(getMapKey(keyPattern, fieldName));
	}
	
	public void setValueDesc(String keyPattern, String fieldName, ValueDesc valueDesc) {
		valueDescMap.put(getMapKey(keyPattern, fieldName), valueDesc);
	}
	
	public static MKeySchema convertKeySchema(KeySchema keySchema) {
		MKeySchema mKeySchema = new MKeySchema();
		
		if(keySchema.getKeyDescs() != null) {
			KeyDesc keyDesc;
			for(int i = 0; i < keySchema.getKeyDescs().size(); i++) {
				keyDesc = keySchema.getKeyDescs().get(i);
				mKeySchema.setValueDesc(keyDesc.getKeyPattern(), keyDesc.getFieldName(), keyDesc.getValDesc());
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
