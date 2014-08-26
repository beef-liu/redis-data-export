package com.beef.redisexport.schema.data;

import java.util.ArrayList;
import java.util.List;

public class KeySchema {
	
	private List<KeyDesc> _keyDescs = new ArrayList<KeyDesc>();
	
	/**
	 * Every KeyDesc is mapping to one key or one key-field. 
	 * @return
	 */
	public List<KeyDesc> getKeyDescs() {
		return _keyDescs;
	}

	public void setKeyDescs(List<KeyDesc> keyDescs) {
		_keyDescs = keyDescs;
	}

}
