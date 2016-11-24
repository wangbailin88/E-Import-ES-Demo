package com.ushine.common.memory;

/**
 * 内存工具
 * 
 * @author Franklin
 *
 */
public class MemoryUtils {

	public static Runtime runtime = Runtime.getRuntime();
	
	public static long useMemory() throws Exception {
		return runtime.totalMemory() - runtime.freeMemory();
	}

}
