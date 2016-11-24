package com.ushine.cluster.hbase.test;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ushine.cluster.hbase.HBaseConnMgr;

/**
 * 获取HBase连接示例及性能测试
 * 
 * @author Franklin
 *
 */
public class HBaseConnMgrTest {

	public static void main(String[] args) throws Exception {
		for(int i=0; i<100; i++) {
			Thread.sleep(1000L);
			new TestThread().start();
		}
	}
	
}

class TestThread extends Thread {
	private static final Logger logger = LoggerFactory.getLogger(TestThread.class);

	private static boolean istestConn = false;
	
	@Override
	public void run() {
		if(istestConn) {
			testHConnection();
		} else {
			testHBaseAdmin();
		}
	}
	
	private void testHConnection() {
		HConnection conn = null;
		try {
			conn = HBaseConnMgr.getConnection();
		} catch (Exception e) {
			logger.error("测试线程:" + this.getName() + "出现异常.", e);
		} finally {
			// HBaseConnMgr.close(conn);
		}
	}
	
	private void testHBaseAdmin() {
		HBaseAdmin admin = null;
		try {
			admin = HBaseConnMgr.createHBaseAdmin();
		} catch (Exception e) {
			logger.error("测试线程:" + this.getName() + "出现异常.", e);
		} finally {
			// HBaseConnMgr.close(admin);
		}
	}
	
}