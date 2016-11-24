package com.ushine.common.config;

/**
 * 系统常量
 * @author franklin
 *
 */
public class Constant {
	
	/**
	 * 信息状态：正常：
	 */
	public static final int NORMAL = 1;
	
	/**
	 * 信息状态：停用
	 */
	public static final int STOP = 0;
	
	/**
	 * 信息状态：删除
	 */
	public static final int DELETE = -1;
	
	/**
	 * HDFS URL
	 */
	public static final String HDFS_URL = "fs.defaultFS";
	
	/**
	 * 数据库驱动
	 */
	public static final String DB_DRIVER = "db.driver";
	
	/**
	 * 数据库连接
	 */
	public static final String DB_URL = "db.url";
	
	/**
	 * 数据库用户名
	 */
	public static final String DB_UNAME = "db.uName";
	
	/**
	 * 数据库密码
	 */
	public static final String DB_PASSD = "db.passd";
	
	/**
	 * HBase Master 服务器
	 */
	public static final String HBASE_ROOT = "hbase.root";
	
	/**
	 * HBase Zookeeper 服务器
	 */
	public static final String ZK_QUORUM = "zookeeper.quorum";
	
	/**
	 * HBase 数据表名称
	 */
	public static final String HBASE_TABLE = "hbase.table";
	
	public static final String HBASE_CF = "INFO";
	
	/**
	 * HBase 创建表时预设Region数量
	 */
	public static final String HBASE_REGION_SIZE = "hbase.region.size";
	
	/**
	 * HBASE 是否关闭日志(HLog)
	 */
	public static final String HBASE_PUT_LOG = "hbase.put.isCloseHLog";

	/**
	 * HBASE 客户端缓存
	 */
	public static final String HBASE_CLIENT_BUFFER = "hbase.client.WriteBufferSize";
	
	/**
	 * HBASE Scan Caching
	 */
	public static final String HBASE_SCAN_CASH = "hbase.scan.cash";
	
	/**
	 * ES 集群名称
	 */
	public static final String ES_NAME = "es.cluster.name";
	
	/**
	 * ES 服务器集群
	 */
	public static final String ES_SERBER = "es.server";
	
	/**
	 * ES 数据库名称
	 */
	public static final String ES_SCHEMA = "es.schema";
	
	/**
	 * ES 时区调整
	 */
	public static final String ES_TIME_ZONE = "es.time.zone";
	
	/**
	 * 数据文件处理线程数量
	 */
	public static final String IMPORT_THREAD_SIZE = "import.thread.size";
	
	/**
	 * 单次数据导入任务数据文件最大处理量
	 */
	public static final String IMPORT_FILE_MAX = "import.file.max";
	
	/**
	 * 导入文件的编码
	 */
	public static final String IMPORT_FILE_ENCODING = "import.file.encoding";
	
	/**
	 * 待导入数据文件存放位置
	 */
	public static final String IMPORT_SOURCE_DIR = "import.source";
	
	/**
	 * 导入成功数据文件存放位置
	 */
	public static final String IMPORT_SUCCEED = "import.succeed";

	/**
	 * 导入失败数据文件存放位置
	 */
	public static final String IMPORT_FAILURE = "import.failure";
	
	/**
	 * 数据导入运行间隔时间
	 */
	public static final String IMPORT_THREAD_SLEEPTIME = "import.thread.sleepTime";
	
	/**
	 * 数据导入HBase批量提交数据量
	 */
	public static final String IMPORT_HBASE_COMMIT = "import.hbase.commit";

	/**
	 * 数据导入Es批量提交数据量
	 */
	public static final String IMPORT_ES_COMMIT = "import.es.commit";
	
}
