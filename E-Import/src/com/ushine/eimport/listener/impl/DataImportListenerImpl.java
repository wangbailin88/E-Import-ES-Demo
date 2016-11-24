package com.ushine.eimport.listener.impl;

import java.io.File;

import com.ushine.common.config.Configured;
import com.ushine.common.config.Constant;
import com.ushine.common.utils.FileUtils;
import com.ushine.common.utils.SpringUtils;
import com.ushine.eimport.event.DataImportEvent;
import com.ushine.eimport.listener.IDataImportListener;
import com.ushine.elogs.service.IDataImportLogService;

public class DataImportListenerImpl implements IDataImportListener {

	public static final String SOURCE_DIR = 
			Configured.getInstance().get(Constant.IMPORT_SOURCE_DIR);
	
	public static final String SUCCEED_DIR = 
			Configured.getInstance().get(Constant.IMPORT_SUCCEED);
	
	public static final String FAILUER_DIR = 
			Configured.getInstance().get(Constant.IMPORT_FAILURE);
	
	private IDataImportLogService logService;
	
	public DataImportListenerImpl() {
		
		logService = (IDataImportLogService) SpringUtils.getBean("dataImportLogService");
	}
	
	@Override
	public void resultHandle(DataImportEvent event) {
		try {
			// 移动文件
			File source = new File(SOURCE_DIR + "/" + event.getFileName());
			File target = null;
			
			if(event.getStatus() == DataImportEvent.SUCCEED) {
				target = new File(SUCCEED_DIR + "/" + event.getFileName());
			}
			
			if(event.getStatus() == DataImportEvent.FAILURE) {
				target = new File(FAILUER_DIR + "/" + event.getFileName());
			}
			
//			if(target != null) {
//				source.renameTo(target);
//				source.delete();
//			}
			
			if(target != null) {
				FileUtils.copy(source, target);
				source.delete();
			}
			
			// 保存信息
			logService.addDataImportLogs(event);
			logService.updateDICountInfo(event.getTotalCount(), 
					event.getRightCount(), event.getErrorCount());
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
