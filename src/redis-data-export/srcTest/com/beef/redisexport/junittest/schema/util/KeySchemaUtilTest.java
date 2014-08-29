package com.beef.redisexport.junittest.schema.util;

import static org.junit.Assert.*;

import org.junit.Test;

import com.beef.redisexport.schema.util.KeyPattern;
import com.beef.redisexport.schema.util.KeySchemaUtil;

public class KeySchemaUtilTest {

	@Test
	public void testParseKeyPattern() {
		testParseKeyPattern("test.{key1}.{key2}{key3}.a01.bbb");
		testParseKeyPattern("{key1}.{key2}.{key3}.{key3}");
		testParseKeyPattern("{key1}.{key2}.{key3}.{key3}.");
		testParseKeyPattern(".{key1}.{key2}.{key3}.{key3}");
		testParseKeyPattern("test.{key1}ab{key2}cd{key3}ef");
		testParseKeyPattern("{key1}ab");
	}

	private void testParseKeyPattern(String keyPattern) {
		KeyPattern pattern = KeySchemaUtil.parseKeyPattern(keyPattern);
		
		System.out.print("pattern(" + keyPattern + ") matchPattern:" + pattern.getKeyMatchPattern());
		if(pattern.getVariateKeyNames() != null && pattern.getVariateKeyNames().size() > 0) {
			for(int i = 0; i < pattern.getVariateKeyNames().size(); i++) {
				System.out.print(" keyName:" + pattern.getVariateKeyNames().get(i));
			}
		}
		System.out.println();
	}
	
}
