package com.ushine.cluster.hbase;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

/**
 * HBase表操作接口，提供创建、删除HBase表及添加、删除、查询数据的方法
 * 本接口只支持单列族
 * 
 * @author Franklin
 *
 */
public interface IHBaseService {

	/**
	 * 创建数据表
	 * @param tableName String 数据库表名
	 * @param columnFamily String 列族名称
	 * @return boolean 成功返回True, 否则返回False
	 */
	public boolean createTable(String tableName, String columnFamily);
	
	/**
	 * 创建数据表 (预先设置分区)
	 * @param tableName String 数据库表名
	 * @param columnFamily String 列族名称
	 * @param splits int 分区数量
	 * @return boolean 成功返回True, 否则返回False
	 */
	public boolean createTable(String tableName, String columnFamily, int split);
	
	/**
	 * 删除数据表
	 * @param tableName String 数据库表名
	 * @return boolean 成功返回True, 否则返回False
	 */
	public boolean deleteTable(String tableName);
	
	/**
	 * 添加一条记录
	 * @param tableName String 数据库表名
	 * @param rowkey String 记录key
	 * @param columnFamily String 列族名称
	 * @param columnValue Map<String, String> 需要插入的数据<字段, 值>
	 * @return boolean 成功返回True, 否则返回False
	 */
	public boolean insertData(String tableName, 
			String rowkey, String columnFamily, Map<String, String> columnValue);
	
	/**
	 * 批量添加记录
	 * @param tableName String 数据库表名
	 * @param columnFamily String 列族名称
	 * @param datas Map<String, Map<String, String>> 需要插入的数据<Key <字段, 值>>
	 * @return boolean 成功返回True, 否则返回False
	 */
	public boolean bulkInsertData(String tableName, 
			String columnFamily, Map<String, Map<String, String>> datas);
	
	/**
	 * 批量添加记录
	 * @param tableName String 数据库表名
	 * @param puts List<Put> 需要插入的数据
	 * @return boolean 成功返回True, 否则返回False
	 */
	public boolean bulkInsertData(String tableName, List<Put> puts);
	
	/**
	 * 删除一条记录
	 * @param tableName String 数据库表名
	 * @param rowkey String 
	 * @return boolean 成功返回True, 否则返回False
	 */
	public boolean deleteData(String tableName, String rowkey);
	
	/**
	 * 批量删除多条记录
	 * @param tableName String 数据库表名
	 * @param rowkeys List<String> rowkeys集合
	 * @return boolean 成功返回True, 否则返回False
	 */
	public boolean bulkIDeleteData(String tableName, List<String> rowkeys);
	
	/**
	 * 批量删除多条记录
	 * @param tableName String 数据库表名
	 * @param rowkeys List<Delete> Delete集合
	 * @return boolean 成功返回True, 否则返回False
	 */
	public boolean bulkIDeleteRowkeys(String tableName, List<Delete> rowkeys);
	
	/**
	 * 通过RowKey获取一条记录
	 * @param tableName String 数据库表名
	 * @param rowkey String
	 * @return Result 返回记录结果, 异常返回null
	 */
	public Result getData(String tableName, String rowkey);

	/**
	 * 通过RowKey批量获取记录
	 * @param tableName String 数据库表名
	 * @param rowkeys List<String> key集合
	 * @return Result[] 返回记录结果集合, 异常返回null
	 */
	public Result[] getDatas(String tableName, List<String> rowkeys);
	
	/**
	 * 查询数据 (scan) 没有记录限制, 数据数量过大会造成内存溢出
	 * @param tableName String 数据库表名
	 * @param startRowKey String 起始rowkey
	 * @return List<Result> 返回记录结果集合, 异常返回null
	 */
	public List<Result> queryData(String tableName, String startRowKey);
	
	/**
	 * 
	 * 查询数据 (scan)
	 * @param tableName String 数据库表名
	 * @param startRowKey String 起始rowkey
	 * @param size int 返回数量
	 * @return List<Result> 返回记录结果集合, 异常返回null
	 */
	public List<Result> queryData(String tableName, String startRowKey, int size);
	
	/**
	 * 查询数据 (scan)
	 * @param tableName String 数据库表名
	 * @param startRowKey String 起始rowkey
	 * @param endRowKey String 停止rowkey
	 * @param size int 返回数量
	 * @return List<Result> 返回记录结果集合, 异常返回null
	 */
	public List<Result> queryData(String tableName, String startRowKey, String endRowKey, int size);
	
}
