package com.ushine.eimport.listener;

import com.ushine.eimport.event.DataImportEvent;

/**
 * 数据导入监听器, 处理数据导入执行完成后的删除.备份.等操作
 * 
 * @author Franklin
 *
 */
public interface IDataImportListener {

	/**
	 * 处理操作
	 * @param event DataImportEvent
	 */
	public void resultHandle(DataImportEvent event);
	
}
