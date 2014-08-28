package com.beef.redisexport.junittest.schema.util;

import org.apache.oro.text.perl.Perl5Util;
import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.MatchResult;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.PatternCompiler;
import org.apache.oro.text.regex.PatternMatcher;
import org.apache.oro.text.regex.PatternMatcherInput;
import org.apache.oro.text.regex.Perl5Compiler;
import org.apache.oro.text.regex.Perl5Matcher;
import org.junit.Ignore;
import org.junit.Test;

public class TestRegex {
	
	@Ignore
	public void test1() {
		try {
			String val = "test001.002_003";
			String regex = "test(.+)\\.(.+)_(.+)";
			
			testRegex1(regex, val);
		} catch(Throwable e) {
			e.printStackTrace();
		}
	}
	
	@Ignore
	public void test2() {
		try {
			String val = "test001.002_abc_003";
			String regex = "test(.+)\\.(.+)_(.+)";
			
			testRegex1(regex, val);
		} catch(Throwable e) {
			e.printStackTrace();
		}
	}

	@Ignore
	public void test3() {
		try {
			String val = "test001.002";
			String regex = "test(.+)\\.(.+)_(.+)";
			
			testRegex1(regex, val);
		} catch(Throwable e) {
			e.printStackTrace();
		}
	}
	
	private static void testRegex1(String regex, String val) throws MalformedPatternException {
		System.out.println("testRegex1() " + regex + " ----> " + val);
		
		PatternCompiler compiler = new Perl5Compiler();
		Pattern pattern = compiler.compile(regex);

		PatternMatcher matcher = new Perl5Matcher();
		MatchResult result;
		PatternMatcherInput matcherInput = new PatternMatcherInput(val);
		if(matcher.contains(matcherInput, pattern)) {
			result = matcher.getMatch();
			
			for(int k = 0; k < result.groups(); k++) {
				System.out.println("result group:" + result.group(k));
			}
		}
	}
}
