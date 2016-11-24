package com.ushine.common.pool;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 任务线程池, 记录当前执行任务及等待任务的信息
 * 
 * @author Franklin
 *
 */
public class ThreadPool {
	private static final Logger logger = LoggerFactory.getLogger(ThreadPool.class);
	
	// 添加运行缓存
	public static final int ADD_RUN = 1;
	
	// 添加等待缓存
	public static final int ADD_WAIT = 0;
	
	// 添加失败
	public static final int ADD_FAILURE = -1;
	
	// 运行线程
	private Map<String, Thread> runTasks = new HashMap<String, Thread>();
	
	// 等待线程
	private Map<String, Thread> waitTasks = new HashMap<String, Thread>();
	
	// 运行线程最大限制 (0=没有限制)
	private int runMaxSize = 0;
	
	// 等待线程最大限制 (0=没有限制)
	private int waitMaxSize = 0;
	
	/**
	 * 初始化线程池
	 * @param runMaxSize int 运行线程最大限制(0=没有限制)
	 * @param waitMaxSize int 等待线程最大限制(0=没有限制)
	 */
	public ThreadPool(int runMaxSize, int waitMaxSize) {
		this.runMaxSize = runMaxSize;
		this.waitMaxSize = waitMaxSize;
	}
	
	/**
	 * 添加新线程
	 * @param id String 线程id
	 * @param thread Thread 
	 * @return int 0=添加运行缓存, 1=添加等待缓存, -1=添加失败
	 */
	public synchronized int addTasks(String id, Thread thread) throws Exception {
		if(runMaxSize==0 || runTasks.size()<runMaxSize) {
			// 加入运行线程
			if(runTasks.get(id) == null) {
				// 如果线程是等待线程，则从等待缓存区删除
				if(waitTasks.get(id) != null) {
					waitTasks.remove(id);
				}
				runTasks.put(id, thread);
				thread.start();
			
				logger.info("线程(" + id 
					+ ")已经添加到线程缓存区. 运行线程缓存区=" + runTasks.size() + "/" 
					+ runMaxSize + ". 等待线程缓存区=" + waitTasks.size() + "/" 
					+ waitMaxSize + ".");
				return ADD_RUN;
			}
		} else if(waitMaxSize==0 || waitTasks.size()<waitMaxSize) {
			// 加入等待线程
			if(waitTasks.get(id) == null) {
				waitTasks.put(id, thread);
			}
			
			logger.info("已经大于运行线程设置:runMaxSize=" + runMaxSize + "线程(" 
					+ id + ")添加到线程等待缓存区. 等待线程缓存区=" + waitTasks.size() + "/" 
					+ waitMaxSize + ".");
			return ADD_WAIT;
		}
		
		// 任务未加入到线程
		logger.warn("线程(" + id 
				+ ")添加线程缓存区失败. 运行线程缓存区=" + runTasks.size() + "/" 
				+ runMaxSize + ". 等待线程缓存区=" + waitTasks.size() + "/" 
				+ waitMaxSize + ".");
		
		return ADD_FAILURE;
	}
	
	/**
	 * 根据id删除运行线程 
	 * @param id String 线程id
	 * @return 删除成功返回True, 否则返回False
	 */
	public synchronized boolean deleteTask(String id) throws Exception {
		if(runTasks.get(id) != null) {
			// 删除运行线程
			runTasks.remove(id);
			// 添加等待线程, 如果添加成功就启动等待线程
			Set<Entry<String, Thread>> entries = waitTasks.entrySet();
			for(Entry<String, Thread> entry : entries) {
				if(addTasks(
						entry.getKey(), entry.getValue())==ADD_RUN) {
					break;
				}
			}
			logger.info("删除运行线程:" + id);
		} else if(waitTasks.get(id) != null) {
			// 删除等待线程
			waitTasks.remove(id);
			logger.info("删除等待线程:" + id);
		} else {
			logger.warn("没有找到需要删除线程:" + id);
			return false;
		}
		
		return true;
	}
	
	/**
	 * 清楚全部运行线程
	 * @return 清楚成功返回True, 否则返回False
	 */
	public boolean clearRunThreads() {
		try {
			runTasks.clear();
			logger.warn("清楚全部运行线程. 运行线程缓存区=" 
					+ runTasks.size() + "/" + runMaxSize + ".");
			return true;
		} catch(Exception e) {
			logger.error("清楚全部运行线程出错.", e);
		}
		return false;
	}
	
	/**
	 * 清楚全部等待线程
	 * @return 清楚成功返回True, 否则返回False
	 */
	public boolean clearWaitThreads() {
		try {
			waitTasks.clear();
			logger.warn("清楚全部等待线程. Size=" 
					+ waitTasks.size() + "/" + waitMaxSize + "");
			return true;
		} catch(Exception e) {
			logger.error("清楚全部等待线程出错.", e);
		}
		return false;
	}
	
	/**
	 * 等待运行线程执行完成
	 */
	public void waitRunThreadComplete()  {
		boolean isComplete = false;
		try {
			while(!isComplete) {
				if(runTasks.size()==0 && waitTasks.size()==0) {
					isComplete = true;
				}
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public Map<String, Thread> getRunTasks() {
		return runTasks;
	}
	
	public Map<String, Thread> getWaitTasks() {
		return waitTasks;
	}
	
	public Thread getTaskById(String id) {
		if(runTasks.get(id) != null) {
			// 删除运行线程
			return runTasks.get(id);
		} else if(waitTasks.get(id) != null) {
			// 删除等待线程
			return waitTasks.get(id);
		}
		
		return null;
	}
	
	public int getRunThreadSize() {
		
		return runTasks.size();
	}
	
	public int getWaitThreadSize() {
		
		return waitTasks.size();
	}
	
	public int getRunMaxSize() {
		return runMaxSize;
	}
	
	public int getWaitMaxSize() {
		return waitMaxSize;
	}
	
}
