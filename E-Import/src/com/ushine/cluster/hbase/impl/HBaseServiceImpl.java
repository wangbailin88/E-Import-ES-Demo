package com.ushine.cluster.hbase.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Resource;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.ushine.cluster.hbase.HBaseConnMgr;
import com.ushine.cluster.hbase.IHBaseService;
import com.ushine.cluster.hbase.split.SplitKeysCalculator;
import com.ushine.common.config.Configured;
import com.ushine.common.config.Constant;

/**
 * IHBaseService接口实现
 * 
 * @author Franklin
 *
 */
@Service("hBaseService")
public class HBaseServiceImpl implements IHBaseService {
	private static final Logger logger = LoggerFactory.getLogger(HBaseServiceImpl.class);
	
	private static final boolean isCloseHLog = 
			Boolean.getBoolean(Configured.getInstance().get(Constant.HBASE_PUT_LOG)); // 是否关闭日志
	
	private static final long WriteBufferSize = 
			Long.parseLong(Configured.getInstance().get(Constant.HBASE_CLIENT_BUFFER)); // 客户端缓存
	
	private static final int scanCash = 
			Integer.parseInt(Configured.getInstance().get(Constant.HBASE_SCAN_CASH));
	
	@Resource(name="partitionRowKeyManager")
	private SplitKeysCalculator calculator;
	
	public boolean createTable(String tableName, String columnFamily) {
		
		return createTable(tableName, columnFamily, 0);
	}

	public boolean createTable(String tableName, String columnFamily, int split) {
		HBaseAdmin hBaseAdmin = null;
		try {
			long startTime = System.currentTimeMillis();
			hBaseAdmin = HBaseConnMgr.createHBaseAdmin();
			
			// 检查数据库中是否存在同名表
			if(hBaseAdmin.tableExists(tableName)) {
				logger.warn("HBase数据库中已经存在" + tableName + "数据表.");
				return false;
			}
			
			// 创建表的描述
			HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
			descriptor.addFamily(new HColumnDescriptor(columnFamily));
			
			if(split <= 0) {
				// 创建表不分区
				hBaseAdmin.createTable(descriptor);
			} else {
				// 预设分区
				calculator.setPartition(split);
				byte [][] splitKeys = calculator.calcSplitKeys();
				
				// 创建表
				hBaseAdmin.createTable(descriptor, splitKeys);
			}
			
			long stopTime = System.currentTimeMillis();
			logger.info("创建HBase数据表(" + tableName + ")成功. 耗时: " + (stopTime-startTime) + "ms");
			return true;
		} catch(Exception e) {
			logger.error("创建HBase数据表(" + tableName + ")出错: ", e);
		} finally {
			HBaseConnMgr.closeHBaseAdmin(hBaseAdmin);
		}
		
		return false;
	}

	public boolean deleteTable(String tableName) {
		HBaseAdmin hBaseAdmin = null;
		try {
			long startTime = System.currentTimeMillis();
			hBaseAdmin = HBaseConnMgr.createHBaseAdmin();
			
			// 检查数据库中是否有该表存在
			if(!hBaseAdmin.tableExists(tableName)) {
				logger.warn("HBase数据库中没有找到" + tableName + "数据表.");
				return false;
			}
					
			// 先关闭, 再删除数据表
			hBaseAdmin.disableTable(tableName);
			hBaseAdmin.deleteTable(tableName);
			
			long stopTime = System.currentTimeMillis();
			logger.info("删除HBase数据表(" + tableName + ")成功. 耗时: " + (stopTime-startTime) + "ms");
			return true;
		} catch(Exception e) {
			logger.error("删除HBase数据表(" + tableName + ")出错: ", e);
		} finally {
			HBaseConnMgr.closeHBaseAdmin(hBaseAdmin);
		}
		
		return false;
	}

	public boolean insertData(String tableName, String rowkey,
			String columnFamily, Map<String, String> columnValue) {
		Map<String, Map<String, String>> datas = new HashMap<String, Map<String,String>>();
		datas.put(rowkey, columnValue);
		
		return bulkInsertData(tableName, columnFamily, datas);
	}

	public boolean bulkInsertData(String tableName,
			String columnFamily, Map<String, Map<String, String>> datas) {
		List<Put> puts = new ArrayList<Put>();
		
		// 解析数据，装入Put后保存
		Set<Entry<String, Map<String, String>>> entries = datas.entrySet();
		for(Entry<String, Map<String, String>> entry : entries) {
			Put put = new Put(Bytes.toBytes(entry.getKey()));
			
			if(isCloseHLog) {
				// Close HLog
				put.setDurability(Durability.SKIP_WAL);
			}
			
			// 装配单条数据
			Map<String, String> data = entry.getValue();
			Set<Entry<String, String>> subEntries = data.entrySet();
			for(Entry<String, String> subEntry : subEntries) {
				// 参数：列族、列、值 
				put.add(Bytes.toBytes(columnFamily), 
						Bytes.toBytes(subEntry.getKey()),Bytes.toBytes(subEntry.getValue()));
			}
			
			puts.add(put); // Add Data To Put
			data.clear();
		}
		
		datas.clear();
		return bulkInsertData(tableName, puts);
	}

	public boolean bulkInsertData(String tableName, List<Put> puts) {
		HConnection conn  = null;
		HTableInterface hTable = null;
		try {
			long startTime = System.currentTimeMillis();
			conn = HBaseConnMgr.getConnection();
			hTable = conn.getTable(tableName);
			hTable.setAutoFlush(false, false);
			hTable.setWriteBufferSize(WriteBufferSize);
			
			// 添加数据并提交
			hTable.put(puts);
			hTable.flushCommits();
			
			long stopTime = System.currentTimeMillis();
			logger.info("批量插入数据到HBase(" + tableName + ")数据表成功. Count=" 
					+ puts.size() + ". 耗时: " + (stopTime-startTime) + "ms");
			return true;
		} catch (Exception e) {
			logger.error("批量插入数据到HBase(" + tableName + ")数据表出错.", e);
		} finally {
			puts.clear();
			HBaseConnMgr.close(conn);
		}
		
		return false;
	}

	public boolean deleteData(String tableName, String rowkey) {
		List<String> rowkeys = new ArrayList<String>();
		rowkeys.add(rowkey);
		
		return bulkIDeleteData(tableName, rowkeys);
	}
	
	public boolean bulkIDeleteData(String tableName, List<String> rowkeys) {
		List<Delete> deletes = new ArrayList<Delete>();
		for(String rowkey : rowkeys) {
			deletes.add(new Delete(Bytes.toBytes(rowkey)));
		}
		
		// rowkeys.clear();
		return bulkIDeleteRowkeys(tableName, deletes);
	}
	
	public boolean bulkIDeleteRowkeys(String tableName, List<Delete> rowkeys) {
		HConnection conn  = null;
		HTableInterface hTable = null;
		try {
			long startTime = System.currentTimeMillis();
			conn = HBaseConnMgr.getConnection();
			hTable = conn.getTable(tableName);
			hTable.setAutoFlush(false, false);
			hTable.setWriteBufferSize(WriteBufferSize);
			
			// 删除数据并提交
			logger.info("准备删除=" + rowkeys.size());
			hTable.delete(rowkeys);
			hTable.flushCommits();
			logger.info("删除失败=" + rowkeys.size());
			
			long stopTime = System.currentTimeMillis();
			logger.info("批量删除HBase(" + tableName + ")数据表数据成功. 耗时: " 
					+ (stopTime-startTime) + "ms");
			return true;
		} catch (Exception e) {
			logger.error("批量删除HBase(" + tableName + ")数据表数据出错.", e);
		} finally {
			rowkeys.clear();
			HBaseConnMgr.close(conn);
		}
		
		return false;
	}

	public Result getData(String tableName, String rowkey) {
		List<String> rowkeys = new ArrayList<String>();
		rowkeys.add(rowkey);
		
		return getDatas(tableName, rowkeys)[0];
	}

	public Result[] getDatas(String tableName, List<String> rowkeys) {
		List<Get> gets = new ArrayList<Get>();
		HConnection conn  = null;
		HTableInterface hTable = null;
		try {
			long startTime = System.currentTimeMillis();
			conn = HBaseConnMgr.getConnection();
			hTable = conn.getTable(tableName);
			
			for(String rowkey : rowkeys) {
				gets.add(new Get(Bytes.toBytes(rowkey)));
			}
			
			Result[] results = hTable.get(gets);
			long stopTime = System.currentTimeMillis();
			logger.info("批量获取HBase(" + tableName + ")数据表数据完成. Count=" 
					+ results.length + ". 耗时: " + (stopTime-startTime) + "ms");
			
			return results;
		} catch (Exception e) {
			logger.error("批量获取HBase(" + tableName + ")数据表数据出错.", e);
		} finally {
			gets.clear();
			HBaseConnMgr.close(conn);
		}
		
		return null;
	}

	public List<Result> queryData(String tableName, String startRowKey) {
		
		return queryData(tableName, startRowKey, null, 0);
	}
	
	public List<Result> queryData(String tableName, String startRowKey, int size) {
		
		return queryData(tableName, startRowKey, null, size);
	}
	
	public List<Result> queryData(String tableName, 
			String startRowKey, String endRowKey, int size) {
		HConnection conn  = null;
		HTableInterface hTable = null;
		ResultScanner resultScanner = null;
		try {
			long startTime = System.currentTimeMillis();
			conn = HBaseConnMgr.getConnection();
			hTable = conn.getTable(tableName);
			Scan scan = new Scan();
			scan.setCaching(scanCash);
			
			if(startRowKey != null) {
				// 设置开始Key
				scan.setStartRow(Bytes.toBytes(startRowKey));
			}
			
			if(endRowKey != null) {
				// 设置结束Key
				scan.setStopRow(Bytes.toBytes(endRowKey));
			}
			
			List<Result> results = new ArrayList<Result>();
			resultScanner = hTable.getScanner(scan);
			for(Result result : resultScanner) {
				// 返回数量=设置返回数量+1, 如果设置数量<=0则表示没有限制
				results.add(result);
				if(size>=1 && results.size()==(size+1)) {
					break;
				}
			}
			
			long stopTime = System.currentTimeMillis();
			logger.info("Scan查询HBase(" + tableName + ")数据表数据完成. startRowKey=" 
					+ startRowKey + ". endRowKey=" + endRowKey + ". Count=" 
					+ results.size() + "/" + size + ". 耗时: " + (stopTime-startTime) + "ms");
			
			return results;
		} catch (Exception e) {
			logger.error("Scan查询HBase(" + tableName + ")数据表数据出错.", e);
		} finally {
			HBaseConnMgr.close(resultScanner);
			HBaseConnMgr.close(conn);
		}
		
		return null;
	}

	public SplitKeysCalculator getCalculator() {
		return calculator;
	}
	
	public void setCalculator(SplitKeysCalculator calculator) {
		this.calculator = calculator;
	}
	
}
