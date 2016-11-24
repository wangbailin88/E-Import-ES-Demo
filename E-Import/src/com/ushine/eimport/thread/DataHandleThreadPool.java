package com.ushine.eimport.thread;

import com.ushine.common.config.Configured;
import com.ushine.common.config.Constant;
import com.ushine.common.pool.ThreadPool;

public class DataHandleThreadPool {

	private static ThreadPool diThreadPool = new ThreadPool(
			Integer.parseInt(Configured.getInstance().get(Constant.IMPORT_THREAD_SIZE)), 0);
	
	public static void addThread(DataHandleThread thread) {
		try {
			diThreadPool.addTasks(thread.getName(), thread);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void removeThread(DataHandleThread thread) {
		try {
			diThreadPool.deleteTask(thread.getName());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static int size() {
		
		return diThreadPool.getRunThreadSize();
	}
	
}
