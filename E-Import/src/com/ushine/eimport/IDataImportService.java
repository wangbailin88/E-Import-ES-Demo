package com.ushine.eimport;

import javax.jws.WebService;

/**
 * 数据导入执行接口, 启动和停止数据导入程序
 * 
 * @author Franklin
 *
 */
@WebService
public interface IDataImportService {

	/**
	 * 启动数据导入程序
	 * @return boolean 成功返回true, 否则false
	 * @throws Exception
	 */
	public boolean startDataImport() throws Exception;
	
	/**
	 * 停止数据导入程序
	 * @return boolean 成功返回true, 否则false
	 * @throws Exception
	 */
	public boolean stopDataImport() throws Exception;
	
	/**
	 * 查看数据导入程序状态
	 * @return boolean 运行中返回True, 否则返回False
	 * @throws Exception
	 */
	public boolean getStatus() throws Exception;
	
}
