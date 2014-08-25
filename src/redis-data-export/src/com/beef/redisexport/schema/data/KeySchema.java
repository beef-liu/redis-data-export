package com.beef.redisexport.schema.data;

import java.util.List;

public class KeySchema {
	private List<KeyDesc> _keyDescs = null;
	
	private List<KeyFieldDesc> _keyFieldDescs = null;

	public List<KeyDesc> getKeyDescs() {
		return _keyDescs;
	}

	public void setKeyDescs(List<KeyDesc> keyDescs) {
		_keyDescs = keyDescs;
	}

	public List<KeyFieldDesc> getKeyFieldDescs() {
		return _keyFieldDescs;
	}

	public void setKeyFieldDescs(List<KeyFieldDesc> keyFieldDescs) {
		_keyFieldDescs = keyFieldDescs;
	}

	
	
}