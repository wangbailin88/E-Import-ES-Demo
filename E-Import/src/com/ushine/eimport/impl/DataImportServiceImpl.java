package com.ushine.eimport.impl;


import javax.jws.WebService;

import org.springframework.stereotype.Service;

import com.ushine.eimport.IDataImportService;
import com.ushine.eimport.thread.DataImportThread;


/**
 * 数据导入接口实现
 * 
 * @author Franklin
 *
 */
@Service("dataImportServiceImpl")
@WebService(endpointInterface="com.ushine.eimport.IDataImportService")
public class DataImportServiceImpl implements IDataImportService {
	
	@Override
	public boolean startDataImport() throws Exception {
		if(!DataImportThread.isRun) {
			DataImportThread diThread = new DataImportThread();
			diThread.start();
			return true;
		}
		
		return false;
	}

	@Override
	public boolean stopDataImport() throws Exception {
		if(DataImportThread.isRun) {
			DataImportThread.stopThread();
			return true;
		}

		return false;
	}

	@Override
	public boolean getStatus() throws Exception {
		
		return DataImportThread.isRun;
	}
	
}
