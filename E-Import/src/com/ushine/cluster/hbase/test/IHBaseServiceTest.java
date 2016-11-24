package com.ushine.cluster.hbase.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ushine.cluster.hbase.IHBaseService;

/**
 * HBaseService接口操作示例及测试
 * 
 * @author Franklin
 *
 */
@Component("hBaseServiceTest")
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "/applicationContext.xml")
public class IHBaseServiceTest {

	public static final String tableName = "WAYBILL2";
	
	public static final String columnFamily = "INFO";
	
	public static final int split = 35;
	
	@Autowired
	private IHBaseService service;
	
	/*
	 * 创建新表
	 */
	@Test
	public void testCreateTable() {
		if(service.createTable(tableName, columnFamily, split)) {
			System.out.println("Create HBase Table Succeed.");
		} else {
			System.out.println("Create HBase Table Error !!!");
		}
	}
	
	/*
	 * 删除数据表
	 */
	@Test
	public void testDeleteTable() {
		if(service.deleteTable(tableName)) {
			System.out.println("Delete HBase Table Succeed.");
		} else {
			System.out.println("Delete HBase Table Error !!!");
		}
	}
	
	/*
	 * 保存数据
	 */
	@Test
	public void testInsertData() {
		// 模拟1条数据
		String rowkey = "testRowKey";
		Map<String, String> columnValue = new HashMap<String, String>();
		for(int j=0; j<10; j++) {
			columnValue.put("字段"+j, "值"+j);
		}
		
		// 提交保存
		if(service.insertData(tableName, rowkey, columnFamily, columnValue)) {
			System.out.println("Insert Data To Table Succeed.");
		} else {
			System.out.println("Insert Data To Table Error !!!");
		}
	}
	
	/*
	 * 批量保存数据
	 */
	@Test
	public void testBulkInsertData() {
		// 模拟10000条数据
		Map<String, Map<String, String>> datas = new HashMap<String, Map<String, String>>();
		for(int i=0; i<100000; i++) {
			Map<String, String> columnValue = new HashMap<String, String>();
			for(int j=0; j<10; j++) {
				columnValue.put("字段"+j, "值"+j);
			}
			datas.put("key"+i, columnValue);
		}
		
		// 提交保存
		if(service.bulkInsertData(tableName, columnFamily, datas)) {
			System.out.println("Bulk Insert Data To Table Succeed.");
		} else {
			System.out.println("Bulk Insert Data To Table Error !!!");
		}
	}
	
	/*
	 * 删除一条数据
	 */
	@Test
	public void testDeleteData() {
		String rowKey = "testRowKey";
		
		// 提交保存
		if(service.deleteData(tableName, rowKey)) {
			System.out.println("Delete Data In Table Succeed.");
		} else {
			System.out.println("Delete Data In Table Error !!!");
		}
	}
	
	/*
	 * 批量删除多条数据
	 */
	@Test
	public void testBulkIDeleteData() {
		List<String> rowkeys = new ArrayList<String>();
		// 模拟数据RowKey
		for(int i=0; i<2000; i++) {
			rowkeys.add("key"+i);
		}
		
		// 提交保存
		if(service.bulkIDeleteData(tableName, rowkeys)) {
			System.out.println("Bulk Delete Data In Table Succeed.");
		} else {
			System.out.println("Bulk Delete Data In Table Error !!!");
		}
	}
	
	/*
	 * 通过RowKey批量获取数据
	 */
	@Test
	public void testGetData() {
		List<String> rowkeys = new ArrayList<String>();
		// 模拟数据RowKey
		for(int i=0; i<1000; i++) {
			rowkeys.add("key"+i);
		}
		
		Result[] results =  service.getDatas(tableName, rowkeys);
		
		if(results != null) {
			for(Result rs : results) {
				if(rs.getRow() != null) {
					System.out.print("rowKey=" + Bytes.toString(rs.getRow()) + ":");
					for(int j=0; j<10; j++) {
						Cell value = rs.getColumnLatestCell(Bytes.toBytes(columnFamily), 
								Bytes.toBytes("字段"+j));
						System.out.print(Bytes.toString(CellUtil.cloneValue(value)) + ",");
					}
					System.out.println(); // 换行
				}
			}
		} else {
			System.out.println("Get Data In Table Error !!!");
		}
	}
	
	/*
	 * 查询数据
	 */
	@Test
	public void testQueryData() {
		String startRowKey = null;
		String endRowKey = null;
		int size = 1000;
		List<Result> results =  service.queryData(tableName, startRowKey, endRowKey, size);
		
		if(results != null) {
			for(Result rs : results) {
				System.out.print("rowKey=" + Bytes.toString(rs.getRow()) + ":");
				for(int j=0; j<10; j++) {
					Cell value = rs.getColumnLatestCell(Bytes.toBytes(columnFamily), 
							Bytes.toBytes("字段"+j));
					System.out.print(Bytes.toString(CellUtil.cloneValue(value)) + ",");
				}
				System.out.println(); // 换行
			}
		} else {
			System.out.println("Get Data In Table Error !!!");
		}
	}
	
	public IHBaseService getService() {
		return service;
	}
	
	public void setService(IHBaseService service) {
		this.service = service;
	}
	
}
