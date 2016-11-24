package com.ushine.cluster.elasticsearch.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ushine.cluster.elasticsearch.ElasticsearchClient;


/**
 * 获取Elasticsearch客户端示例及性能测试
 * :测试发起100个线程去循环获取Elasticsearch连接
 * 
 * @author Franklin
 *
 */
public class ElasticsearchClientTest {

	public static void main(String[] args) {
		try {
			for(int i=0; i<1000; i++) {
				new TestClientThread().start(); // 发起查询线程
				// Thread.sleep(1000);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}

class TestClientThread extends Thread {
	private static final Logger logger = LoggerFactory.getLogger(TestClientThread.class);
	
	@Override
	public void run() {
		try {
			ElasticsearchClient.getTransportClient();
		} catch (Exception e) {
			logger.error("测试线程" + this.getName() + "执行出错.", e);
		} finally {
			// ElasticsearchClient.close();
		}
	}
	
}