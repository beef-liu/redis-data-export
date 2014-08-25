package com.beef.redisexport.schema.util;

import java.util.HashMap;

import com.beef.redisexport.schema.data.KeyDesc;
import com.beef.redisexport.schema.data.KeyFieldDesc;
import com.beef.redisexport.schema.data.KeySchema;

public class MKeySchema {
	/**
	 * key:keyPattern value: KeyDesc or KeyFieldDesc
	 */
	private HashMap<String, Object> keyDescMap = new HashMap<String, Object>();

	public HashMap<String, Object> getKeyDescMap() {
		return keyDescMap;
	}

	public void setKeyDescMap(HashMap<String, Object> keyDescMap) {
		this.keyDescMap = keyDescMap;
	}

	public static MKeySchema convertKeySchema(KeySchema keySchema) {
		MKeySchema mKeySchema = new MKeySchema();
		
		if(keySchema.getKeyDescs() != null) {
			KeyDesc keyDesc;
			for(int i = 0; i < keySchema.getKeyDescs().size(); i++) {
				keyDesc = keySchema.getKeyDescs().get(i);
				mKeySchema.getKeyDescMap().put(keyDesc.getKeyPattern(), keyDesc);
			}
		}
		
		if(keySchema.getKeyFieldDescs() != null) {
			KeyFieldDesc keyFieldDesc;
			for(int i = 0; i < keySchema.getKeyFieldDescs().size(); i++) {
				keyFieldDesc = keySchema.getKeyFieldDescs().get(i);
				mKeySchema.getKeyDescMap().put(keyFieldDesc.getKeyPattern(), keyFieldDesc);
			}
		}
		
		return mKeySchema;
	}
	
}
