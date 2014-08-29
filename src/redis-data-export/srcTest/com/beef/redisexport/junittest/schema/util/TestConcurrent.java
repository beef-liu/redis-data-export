package com.beef.redisexport.junittest.schema.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.junit.Test;

public class TestConcurrent {

	private Map<String, ReentrantReadWriteLock> _lockMap = new ConcurrentHashMap<String, ReentrantReadWriteLock>();
	private Map<String, TestResource> _resourceMap = new HashMap<String, TestConcurrent.TestResource>();
	private Random _random = new Random(System.currentTimeMillis());
	
	public static void main(String[] args) {
		try {
			int threadGroupCount = 5;
			TestConcurrent test = new TestConcurrent(threadGroupCount);
			//test.test1Group(0);
			
			for(int i = 0; i < threadGroupCount; i++) {
				test.test1Group(i);
			}

			//Thread.sleep(100000);
		} catch(Throwable e) {
			e.printStackTrace();
		}
	}
	
	public TestConcurrent(int threadGroupCount) {
		for(int i = 0; i < threadGroupCount; i++) {
			_lockMap.put(String.valueOf(i), new ReentrantReadWriteLock());
			_resourceMap.put(String.valueOf(i), new TestResource());
		}
	}
	
	public void test1Group(int threadGroupNum) {
		try {
			System.out.println("test1() [" + threadGroupNum + "] begin -------------------------");
			
			for(int i = 0; i < 100; i++) {
				MyThread t = new MyThread(threadGroupNum, i);
				t.start();
			}
			
			System.out.println("test1() [" + threadGroupNum + "] end -------------------------");
		} catch(Throwable e) {
			e.printStackTrace();
		}
	}
	
	
	private ReentrantReadWriteLock getThreadGroupLock(int threadGroupNum) {
		return _lockMap.get(String.valueOf(threadGroupNum));
	}
	
	private class MyThread extends Thread {
		private int _threadGroupNum;
		private int _threadNum;
		
		public MyThread(int threadGroupNum, int threadNum) {
			_threadGroupNum = threadGroupNum;
			_threadNum = threadNum;
		}
		
		@Override
		public void run() {
			try {
				Thread.sleep(_random.nextInt(1000) + 100);
				
				ReentrantReadWriteLock lock = getThreadGroupLock(_threadGroupNum);
				
				TestResource resource = _resourceMap.get(String.valueOf(_threadGroupNum));

				lock.readLock().lock();
				System.out.println("Thread[" + _threadGroupNum + "][" + _threadNum + "] resourceCount:" + resource.getResourceCount() + " --------------------------------------");
				Thread.sleep(_random.nextInt(10) + 10);
				lock.readLock().unlock();
				
				
				lock.writeLock().lock();
				System.out.println("Thread[" + _threadGroupNum + "][" + _threadNum + "] write lock >>>>>>> resourceCount:" + resource.getResourceCount());
				for(int i = 0; i < 1000; i++) {
					resource.increaseResource();
				}
				Thread.sleep(_random.nextInt(1000) + 10);
				System.out.println("Thread[" + _threadGroupNum + "][" + _threadNum + "] write unLock <<<<< resourceCount:" + resource.getResourceCount());
				lock.writeLock().unlock();
			} catch(Throwable e) {
				e.printStackTrace();
			}
		}
		
	}
	
	private static class TestResource {
		private int _someKindResource = 0;
		
		public void increaseResource() {
			_someKindResource++;
		}
		
		public int getResourceCount() {
			return _someKindResource;
		}
	}
}
