package com.ushine.cluster.elasticsearch;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ushine.common.config.Configured;
import com.ushine.common.config.Constant;

/**
 * ElasticSearch客户端管理器，应用程序通过该接口获取ElasticSearch客户端
 * 
 * @author Frnaklin
 *
 */
public class ElasticsearchClient {
	private static final Logger logger = LoggerFactory.getLogger(ElasticsearchClient.class);
	
	private static String clusterName; // ElasticSearch集群名称
	
	private static String serverNames[]; // ElasticSearch集群服务器名称
	
	private static int port = 9300; // ElasticSearch集群端口
	
	private static Settings settings;
	
	private static TransportClient client;
	
	static {
		clusterName = Configured.getInstance().get(Constant.ES_NAME);
		serverNames = Configured.getInstance().get(Constant.ES_SERBER).split(",");
		settings = ImmutableSettings.settingsBuilder()
				.put("cluster.name", clusterName).build();
		
		logger.info("初始化ElasticSearch客户端配置信息: clusterName=" 
				+ clusterName + ", serverSize=" + serverNames.length);
	}
	
	/**
	 * 获取ElasticSearch客户端
	 * @return TransportClient 获取失败返回为null
	 */
	public static synchronized TransportClient getTransportClient() {
		try {
			if(client==null) {
				client = new TransportClient(settings);
				for(String serverName : serverNames) {
					client.addTransportAddress(
							new InetSocketTransportAddress(serverName, port));
				}
			} 
			logger.info("获取ElasticSearch客户端成功:" + client);
		} catch (Exception e) {
			logger.error("获取ElasticSearch客户端出错.", e);
			close();
		}
		
		return client;
	}
	
	/**
	 * 关闭ElasticSearch客户端
	 */
	public static synchronized void close() {
		try {
			if(client!=null) {
				client.close();
				logger.info("关闭ElasticSearch客户端:" + client);
				client=null;
			}
		} catch(Exception e) {
			logger.error("ElasticSearch客户端(" + client + ")关闭出错.", e);
		}
	}
	
}
