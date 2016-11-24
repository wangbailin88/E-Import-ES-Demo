package com.ushine.eimport.test;

import org.apache.cxf.jaxws.JaxWsProxyFactoryBean;

import com.ushine.eimport.IDataImportService;

/**
 * 测试手动启动数据导入程序
 * 
 * @author Franklin
 *
 */
public class DataImportServiceTest {
	
	private IDataImportService diService;
	
	public DataImportServiceTest() {
		//创建WebService客户端代理工厂
		JaxWsProxyFactoryBean factory = new JaxWsProxyFactoryBean();
		//注册WebService接口
		factory.setServiceClass(IDataImportService.class);
		//设置WebService地址
		factory.setAddress("http://127.0.0.1:8080/eimport/ws/diService");
		diService = (IDataImportService)factory.create();
	}
	
	public void testStart() {
		try {
			System.out.println("数据导入启动: " + diService.startDataImport());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void testStop() {
		try {
			System.out.println("数据导入启动: " + diService.stopDataImport());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws Exception {
		new DataImportServiceTest().testStart();
	}

}
