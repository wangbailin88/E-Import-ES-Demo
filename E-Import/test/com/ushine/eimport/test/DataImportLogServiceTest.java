package com.ushine.eimport.test;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ushine.eimport.event.DataImportEvent;
import com.ushine.elogs.service.IDataImportLogService;

@Component("dataImportLogServiceTest")
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "/applicationContext.xml")
public class DataImportLogServiceTest {

	@Autowired
	private IDataImportLogService service;
	
	@Test
	public void testAddDataImportLogs() {
		try {
			DataImportEvent event = new DataImportEvent();
			event.setFileName("测试文件.csv");
			event.setStartTime("2015-01-01 00:00:00");
			event.setEndTime("2015-01-01 23:59:59");
			event.setTotalCount(50000);
			event.setRightCount(48000);
			event.setErrorCount(2000);
			event.setStatus(DataImportEvent.SUCCEED);
			
			service.addDataImportLogs(event);
			service.updateDICountInfo(event.getTotalCount(), 
					event.getRightCount(), event.getErrorCount());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testUpdateDICountInfo() {
		try {
			service.updateDICountInfo(0, 0, 0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public IDataImportLogService getService() {
		return service;
	}
	
}
