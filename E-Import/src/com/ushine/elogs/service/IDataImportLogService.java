package com.ushine.elogs.service;

import com.ushine.eimport.event.DataImportEvent;

/**
 * 数据导入日志接口, 添加导入日志, 并更新导入统计信息
 * 
 * @author Franklin
 *
 */
public interface IDataImportLogService {
	
	/**
	 * 添加导入日志
	 * @param dInfo DImoprtInfo 记录文件信息
	 * @throws Exception
	 */
	public void addDataImportLogs(DataImportEvent dInfo) throws Exception;
	
	/**
	 * 更新导入统计信息
	 * @param totalCount long 添加的数据总量
	 * @param rightCount long 添加的数据正确量
	 * @param errorCount long 添加的数据错误量
	 */
	public void updateDICountInfo(long totalCount, 
			long rightCount, long errorCount) throws Exception;
	
}
