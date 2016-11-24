package com.ushine.eimport.thread;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ushine.common.config.Configured;
import com.ushine.common.config.Constant;
import com.ushine.common.utils.DateUtils;
import com.ushine.eimport.listener.impl.DataImportListenerImpl;

/**
 * 数据导入线程, 定时检查新数据文件, 并分发给数据文件处理线程
 * 检查过程发生异常线程自动停止
 * 
 * @author Franklin
 *
 */
public class DataImportThread extends Thread {
	private static final Logger logger = LoggerFactory.getLogger(DataImportThread.class);
	
	// 数据文件处理线程数量
	private static final int THREAD_SIZE = 
			Integer.parseInt(Configured.getInstance().get(Constant.IMPORT_THREAD_SIZE));
		
	// 单次导入处理的最大文件数量
	private static final int FILE_MAX = 
			Integer.parseInt(Configured.getInstance().get(Constant.IMPORT_FILE_MAX));
	
	// 文件源
	private static final String SOURCE_DIR = 
			Configured.getInstance().get(Constant.IMPORT_SOURCE_DIR);
	
	// 数据导入运行间隔时间
	private static final int THREAD_SLEEPTIME = 
			Integer.parseInt(Configured.getInstance().get(Constant.IMPORT_THREAD_SLEEPTIME));
	
	// 线程中装载文件的容器
	private Map<Integer, List<File>> container = new HashMap<Integer, List<File>>();
	
	public static boolean isRun = false;
	
	public DataImportThread() {
		initContainer();
		isRun = true;
	}
	
	@Override
	public void run() {
		String startDate = DateUtils.currTimeToString();
		logger.info("数据导入应用程序启动. 开始时间=" + startDate);
		try {
			while(isRun) {
				// 等待上次的导入任务执行完成
				if(DataHandleThreadPool.size() > 0) {
					continue;
				}
				
				// 获取需要导入的数据文件
				File dataDir = new File(SOURCE_DIR);
				File[] dataFiles = dataDir.listFiles();
				
				if(dataFiles.length <= 0) {
					logger.warn("没有检测到需要导入的数据文件, 下次检测" + (THREAD_SLEEPTIME/1000) + "s后");
				} else {
					// 将文件分发给数据文件处理线程
					for(int i=0; i<dataFiles.length && i<FILE_MAX;) {
						for(int j=0; (j<THREAD_SIZE && i<dataFiles.length) && i<FILE_MAX; i++,j++) {
							if(!"temp.csv".equalsIgnoreCase(dataFiles[i].getName())) {
								container.get(j).add(dataFiles[i]);
							}
						}
					}
					
					// 启动数据导入线程, 有数据文件的线程才会被启用
					for(int i=0; i<THREAD_SIZE; i++) {
						if(container.get(i).size() > 0) {
							DataHandleThread handleThread = new DataHandleThread(container.get(i));
							handleThread.addDataImportListener(new DataImportListenerImpl());
							DataHandleThreadPool.addThread(handleThread);
						}
					}
					
					initContainer();
				}
				sleep(THREAD_SLEEPTIME);
			}
		} catch (Exception e) {
			logger.error("数据导入应用程序发生异常, 自动停止...", e);
		} finally {
			String endDate = DateUtils.currTimeToString();
			logger.info("数据导入应用程序启动. 开始时间=" + startDate + ", 停止时间=" + endDate);
		}
	}
	
	/**
	 *  初始化容器
	 */
	private void initContainer() {
		for(int i=0; i<THREAD_SIZE; i++) {
			container.put(i, new ArrayList<File>());
		}
	}
	
	/**
	 * 手动停止
	 */
	public static void stopThread() {
		isRun = false;
	}
	
}
