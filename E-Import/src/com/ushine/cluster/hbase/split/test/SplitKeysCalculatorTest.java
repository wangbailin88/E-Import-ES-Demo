package com.ushine.cluster.hbase.split.test;

import javax.annotation.Resource;

import junit.framework.TestCase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ushine.cluster.hbase.HBaseConnMgr;
import com.ushine.cluster.hbase.split.SplitKeysCalculator;

/**
 * 运行后, 查看建表结果：执行 scan 'hbase:meta'
 * 
 * @author user
 *
 */
@Component("splitKeysCalculatorTest")
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "/applicationContext.xml")
public class SplitKeysCalculatorTest extends TestCase {

	private TableName tableName = TableName.valueOf("test_tmp_table");
	
	@Resource(name="hashChoreWoker")
	private SplitKeysCalculator worker;
	
	@Resource(name="partitionRowKeyManager")
	private SplitKeysCalculator rkManager;
	
	/*
	 * Hash方式预设分区
	 */
	@Test
	public void testHashAndCreateTable() throws Exception{
		worker.setPartition(7); 
		byte [][] splitKeys = worker.calcSplitKeys();
		
		HBaseAdmin hBaseAdmin = HBaseConnMgr.createHBaseAdmin();
		HTableDescriptor tableDesc = new HTableDescriptor(tableName);
		HColumnDescriptor columnDesc = new HColumnDescriptor(Bytes.toBytes("info"));
		columnDesc.setMaxVersions(1);
		tableDesc.addFamily(columnDesc);
		
		if(hBaseAdmin.tableExists(tableName)) {
			try {
				hBaseAdmin.disableTable(tableName);
			} catch(Exception e) {
				e.printStackTrace();
			}
			hBaseAdmin.deleteTable(tableName);
		}
		
		hBaseAdmin.createTable(tableDesc ,splitKeys);
		hBaseAdmin.close();
	}
	
	/*
	 * Partition方式预设分区
	 */
	@Test
	public void testPartitionAndCreateTable() throws Exception{
		rkManager.setPartition(7); 
		byte [][] splitKeys = rkManager.calcSplitKeys();
        
		HBaseAdmin hBaseAdmin = HBaseConnMgr.createHBaseAdmin();
		 
		HTableDescriptor tableDesc = new HTableDescriptor(tableName);
		HColumnDescriptor columnDesc = new HColumnDescriptor(Bytes.toBytes("info"));
		columnDesc.setMaxVersions(1);
		tableDesc.addFamily(columnDesc);
		
		if(hBaseAdmin.tableExists(tableName)) {
			try {
				hBaseAdmin.disableTable(tableName);
			} catch(Exception e) {
				e.printStackTrace();
			}
			hBaseAdmin.deleteTable(tableName);
		}
		
		hBaseAdmin.createTable(tableDesc ,splitKeys);
		hBaseAdmin.close();
    }

	public SplitKeysCalculator getRkManager() {
		return rkManager;
	}
	
	public void setRkManager(SplitKeysCalculator rkManager) {
		this.rkManager = rkManager;
	}
	
	public SplitKeysCalculator getWorker() {
		return worker;
	}
	
	public void setWorker(SplitKeysCalculator worker) {
		this.worker = worker;
	}
	
}
