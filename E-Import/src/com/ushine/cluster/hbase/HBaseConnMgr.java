package com.ushine.cluster.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ushine.common.config.Configured;
import com.ushine.common.config.Constant;

/**
 * HBase连接管理器, 提供获取Configuration. HConnection. HBaseAdmin的方法
 * 
 * @author Franklin
 *
 */
public class HBaseConnMgr {
	private static final Logger logger = LoggerFactory.getLogger(HBaseConnMgr.class);
	
	private static String root; // HBase Master 地址
	
	private static String quorum; // ZK
	
	private static Configuration config;
	
	static {
		root = Configured.getInstance().get(Constant.HBASE_ROOT);
		quorum = Configured.getInstance().get(Constant.ZK_QUORUM);
		
		config = HBaseConfiguration.create();
		config.set("hbase.rootdir", root);
		config.set("hbase.zookeeper.quorum", quorum);
		
		logger.info("初始化HBase连接管理器配置信息, hbase.root=" + root + ", quorum:" + quorum);
	}
	
	/**
	 * 获取HBase Configuration
	 * @return Configuration
	 */
	public static Configuration getConfiguration() {
		
		return config;
	}
	
	/**
	 * 获取HBase连接
	 * @return HConnection 返回连接，如出现异常返回null
	 */
	public static HConnection getConnection() {
		HConnection conn = null;
		try {
			conn = HConnectionManager.createConnection(config);
			logger.info("获取HBase连接成功: " + conn);
		} catch (Exception e) {
			logger.error("获取HBase连接出错.", e);
			close(conn);
		}
		
		return conn;
	}
	
	/**
	 * 关闭HBase连接
	 * @param conn HConnection
	 */
	public static void close(HConnection conn) {
		try {
			if(conn!=null && !conn.isClosed()) {
				conn.close();
				logger.info("关闭HBase连接完成: " + conn);
			}
		} catch (Exception e) {
			logger.error("关闭HBase连接(" + conn + ")出错.", e);
		}
	}
	
	/**
	 * 创建HBaseAdmin
	 * @return HBaseAdmin 返回对象，如出现异常返回null
	 */
	public static HBaseAdmin createHBaseAdmin() {
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(config);
			logger.info("创建HBaseAdmin成功:" + admin);
		} catch (Exception e) {
			logger.error("创建HBaseAdmin出错.", e);
			closeHBaseAdmin(admin);
		}
		
		return admin;
	}
	
	/**
	 * 关闭HBaseAdmin
	 * @param admin HBaseAdmin
	 */
	public static void closeHBaseAdmin(HBaseAdmin admin) {
		try {
			if(admin!=null) {
				admin.close();
				logger.info("关闭HBaseAdmin完成:" + admin);
			}
		} catch (Exception e) {
			logger.error("关闭HBaseAdmin(" + admin + ")出错.", e);
		}
	}
	
	/**
	 * 关闭HBase Table
	 * @param hTable HTableInterface
	 */
	public static void close(HTableInterface hTable) {
		try {
			if(hTable!=null) {
				hTable.close();
				logger.info("关闭HBase数据表完成:" + hTable);
			}
		} catch (Exception e) {
			logger.error("关闭HBase数据表(" + hTable + ")出错.", e);
		}
	}
	
	/**
	 * 关闭HBase ResultScanner
	 * @param resultScanner ResultScanner
	 */
	public static void close(ResultScanner resultScanner) {
		try {
			if(resultScanner!=null) {
				resultScanner.close();
				logger.info("关闭HBase ResultScanner完成:" + resultScanner);
			}
		} catch (Exception e) {
			logger.error("关闭HBase ResultScanner(" + resultScanner + ")出错.", e);
		}
	}
}
