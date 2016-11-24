package com.ushine.elogs.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.ushine.dao.IBaseDao;
import com.ushine.eimport.event.DataImportEvent;
import com.ushine.elogs.model.DICountInfo;
import com.ushine.elogs.service.IDataImportLogService;


@Transactional
@Service("dataImportLogService")
public class DataImportLogServiceImpl implements IDataImportLogService {

	@Autowired
	private IBaseDao<DataImportEvent, String> logsDao;
	
	@Autowired
	private IBaseDao<DICountInfo, String> countDao;
	
	@Override
	public void addDataImportLogs(DataImportEvent dInfo) throws Exception {
		
		logsDao.save(dInfo);
	}
	
	@Override
	public void updateDICountInfo(long totalCount, 
			long rightCount, long errorCount) throws Exception {
		List<DICountInfo> countInfos = countDao.findAll(DICountInfo.class);
		
		if(countInfos!=null && countInfos.size()>0) {
			DICountInfo info = countInfos.get(0);
			info.setTotalCount(info.getTotalCount() + totalCount);
			info.setRightCount(info.getRightCount() + rightCount);
			info.setErrorCount(info.getErrorCount() + errorCount);
			
			countDao.update(info);
		} else {
			DICountInfo info = new DICountInfo();
			info.setTotalCount(totalCount);
			info.setRightCount(rightCount);
			info.setErrorCount(errorCount);
			
			countDao.save(info);
		}
		
	}
	
	public IBaseDao<DICountInfo, String> getCountDao() {
		return countDao;
	}
	
	public IBaseDao<DataImportEvent, String> getLogsDao() {
		return logsDao;
	}
	
}
