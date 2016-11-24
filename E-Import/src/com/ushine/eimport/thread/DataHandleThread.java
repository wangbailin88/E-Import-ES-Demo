package com.ushine.eimport.thread;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ushine.cluster.elasticsearch.IElasticsearchServicve;
import com.ushine.cluster.hbase.IHBaseService;
import com.ushine.common.config.Configured;
import com.ushine.common.config.Constant;
import com.ushine.common.utils.DateUtils;
import com.ushine.common.utils.RandomUtils;
import com.ushine.common.utils.SpringUtils;
import com.ushine.common.utils.StringUtils;
import com.ushine.eimport.event.DataImportEvent;
import com.ushine.eimport.listener.IDataImportListener;
import com.ushine.eimport.relolver.DataModelResolver;

/**
 * 数据处理线程, 将分配到的数据文件存入ES和HBase
 * 
 * @author Franklin
 *
 */
public class DataHandleThread extends Thread {
	private static final Logger logger = LoggerFactory.getLogger(DataHandleThread.class);
	
	private static String[] hbaseColumns = 
			DataModelResolver.getInstance().getHBaseColumns();
	
	private static String[] esIndexs = 
			DataModelResolver.getInstance().getIndexFields();
	
	private static String[] esIndexType = 
			DataModelResolver.getInstance().getColType();
	
	// ES_SCHEMA
	private static final String indexName = 
			Configured.getInstance().get(Constant.ES_SCHEMA);
	
	// HBASE_TABLE
	private static final String tableName = 
			Configured.getInstance().get(Constant.HBASE_TABLE);
	
	private static final int timeZone = 
			Integer.parseInt(Configured.getInstance().get(Constant.ES_TIME_ZONE));
	
	private static final int regionSize = 
			Integer.parseInt(Configured.getInstance().get(Constant.HBASE_REGION_SIZE));
	
	private static int hbaseCommitSize = 
			Integer.parseInt(Configured.getInstance().get(Constant.IMPORT_HBASE_COMMIT));
	
	private static int esCommitSize = 
			Integer.parseInt(Configured.getInstance().get(Constant.IMPORT_ES_COMMIT));
	
	// 文件编码
	private static String encoding = 
			Configured.getInstance().get(Constant.IMPORT_FILE_ENCODING);
	
	private static boolean isCloseHLog = 
			Boolean.parseBoolean(Configured.getInstance().get(Constant.HBASE_PUT_LOG));
	
	// 数据文件
	private List<File> dataFiles;
	
	// 数据文件总数
	private int file_total = 0;
		
	// 导入成功数据文件
	private int file_succeed = 0;
	
	// 数据导入事件类
	private DataImportEvent event; 
	
	// 监听器
	private IDataImportListener listener;
	
	private BufferedReader reader;
	
	private IElasticsearchServicve esServicve;
	
	private IHBaseService hbaService;
	
	// Es缓存区
	private Map<String, XContentBuilder> contents = new HashMap<String, XContentBuilder>();
	
	// HBase缓存区
	private List<Put> puts = new ArrayList<Put>();
	
	public DataHandleThread(List<File> dataFiles) {
		this.dataFiles = dataFiles;
		file_total = dataFiles.size();
		
		esServicve = (IElasticsearchServicve)SpringUtils.getBean("elasticsearchServicve");
		hbaService = (IHBaseService) SpringUtils.getBean("hBaseService");
	}
	
	public void addDataImportListener(IDataImportListener listener) {
		this.listener = listener;
	}
	
	@Override
	public void run() {
		logger.info("线程:" + this.getName() + ", 负责导入(" + file_total + ")文件");
		long startTime = System.currentTimeMillis();
		try {
			for(File dataFile : dataFiles) {
				long sTime = System.currentTimeMillis();
				event = new DataImportEvent(dataFile.getName());
				event.setStartTime(DateUtils.currTimeToString());
				
				// 获取ES的Type(物流公司)
				String searchType = null;
				String cCode = null;
				String[] tmp = dataFile.getName().split("-");
				if(tmp==null || tmp.length<2) {
					throw new Exception("文件名错误，不能识别公司代码.");
				} else {
					// searchType = tmp[0];
					searchType = tmp[1];
					cCode = tmp[1];
				}
				
				reader = new BufferedReader(new InputStreamReader(
						new FileInputStream(dataFile), encoding));
				
				String id = null;
				String tmpStr = null;
				// 循环读取文件中的数据, 在进行批量提交
				while((tmpStr=reader.readLine()) != null) {
					String[] tmpStrArray = tmpStr.split("<=>");
					String[] data = tmpStrArray[1].split(",");
					event.setTotalCount(event.getTotalCount()+1);
					
					// 检查数据长度
					if(StringUtils.isNull(data) || data.length!=hbaseColumns.length) {
						event.setErrorCount(event.getErrorCount()+1);
						continue;
					} else {
						event.setRightCount(event.getRightCount()+1);
						// 生成ID
						// id = RandomUtils.getRandomInt(regionSize) + "_" + cCode + data[0];
						id = tmpStrArray[0];
					}
					
					saveHBaseData(Bytes.toBytes(id), data);
					if(puts.size() == hbaseCommitSize) {
						commitHBaseData();
					}
					
					saveEsIndex(id, data);
					if(contents.size() == esCommitSize) {
						commitEsData(searchType);
					}
				}
				
				if(puts.size() > 0) {
					commitHBaseData();
				}
				
				if(contents.size() > 0) {
					commitEsData(searchType);
				}
				
				// 完成文件操作设置状态并触发事件
				long eTime = System.currentTimeMillis();
				logger.info("数据文件:" + event.getFileName() + "导入完成(" 
						+ (++file_succeed) + "/" + file_total +"), 耗时:" + (eTime-sTime)/1000 + "s.");
				event.setStatus(DataImportEvent.SUCCEED);
				event.setEndTime(DateUtils.currTimeToString());
				resultHandle(event);
			}
			long endTime = System.currentTimeMillis(); 
			logger.info("共完成(" + file_total + ")个数据文件导入, 耗时:" + (endTime-startTime)/1000 + "s.");
		} catch (Exception e) {
			logger.error("数据导入线程" + this.getName() + "发生异常. 数据文件=" 
					+ file_total + ", 成功导入=" + file_succeed + ", 失败=" + (file_total-file_succeed), e);
			event.setStatus(DataImportEvent.FAILURE);
			event.setEndTime(DateUtils.currTimeToString());
			resultHandle(event);
		} finally {
			DataHandleThreadPool.removeThread(this);
		}
	}
	
	/*
	 * 添加数据到Es缓存
	 */
	private void saveEsIndex(String id, String[] data) throws Exception {
		XContentBuilder document = XContentFactory.jsonBuilder();
		document.startObject();
		for(int j=0; j<data.length; j++) {
			if(esIndexs[j].equals("null")) {
				// 该字段没有配置
				continue;
			}
			if(esIndexType[j].equals("date")) {
				// 转换为日期型
				Calendar calendar = Calendar.getInstance();
				calendar.setTime(DateUtils.parseDate(data[j], null));
				calendar.setTimeInMillis(calendar.getTimeInMillis() + timeZone * 3600 * 1000);
				
				document.field(esIndexs[j], calendar.getTime());
			} else if(esIndexType[j].equals("int")) {
				// 转换为整型
				document.field(esIndexs[j], StringUtils.toInt(data[j]));
			} else if(esIndexType[j].equals("double")) {
				// 转换为双精浮点型
				document.field(esIndexs[j], Double.parseDouble(data[j]));
			} else if(esIndexType[j].equals("float")) {
				// 转换为浮点型
				document.field(esIndexs[j], Float.parseFloat(data[j]));
			} else {
				document.field(esIndexs[j], data[j]);
			}
		}
		document.endObject();
		
		contents.put(id, document);
	}
	
	/*
	 * 批量提交Es
	 */
	private void commitEsData(String searchType) throws Exception {
		
		esServicve.bulkInsertDocument(indexName, searchType, contents);
		contents.clear();
	}
	
	/*
	 * 添加数据到HBase缓存
	 */
	private void saveHBaseData(byte[] id, String[] data) {
		Put put = new Put(id);
		if(isCloseHLog) {
			// Close HLog
			put.setDurability(Durability.SKIP_WAL);
		}
		
		for(int i=0; i<data.length; i++) {
			if(hbaseColumns[i].equals("null")) {
				// 该字段没有配置
				continue;
			}
			put.add(Bytes.toBytes(Constant.HBASE_CF), 
					Bytes.toBytes(hbaseColumns[i]), Bytes.toBytes(data[i]));
		}
		
		puts.add(put);
	}
	
	/*
	 * 批量提交HBase
	 */
	private void commitHBaseData() throws Exception {
		
		hbaService.bulkInsertData(tableName, puts);
		puts.clear();
	}
	
	/*
	 * 触发事件
	 */
	private void resultHandle(DataImportEvent event) {
		close();
		if(listener != null) {
			listener.resultHandle(event);
		}
	}
	
	/*
	 * 关闭文件流
	 */
	private void close() {
		try {
			if(reader != null) {
				reader.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
