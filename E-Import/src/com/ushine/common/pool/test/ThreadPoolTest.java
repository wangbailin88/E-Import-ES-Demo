package com.ushine.common.pool.test;

import com.ushine.common.pool.ThreadPool;
import com.ushine.common.utils.RandomUtils;

/**
 * 测试ThreadPool
 * 
 * @author Franklin
 *
 */
public class ThreadPoolTest {
	
	// 线程池
	private static ThreadPool threadPool = new ThreadPool(10, 0);
	
	public void testThreadPool() {
		try {
			for(int i=0; i<100; i++) {
				TestThread testThread = 
						new TestThread(RandomUtils.getRandomString(32));
				threadPool.addTasks(testThread.getName(), testThread);
				Thread.sleep(1000);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static ThreadPool getThreadPool() {
		
		return threadPool;
	}
	
	public static void main(String[] args) {
		
		new ThreadPoolTest().testThreadPool();
	}

}

/**
 * 测试线程
 * @author Franklin
 *
 */
class TestThread extends Thread {
	
	public TestThread(String name) {
		setName(name);
	}
	
	@Override
	public void run() {
		try {
			sleep(20000);
			
			// 清除线程
			ThreadPoolTest.
				getThreadPool().deleteTask(getName());
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}