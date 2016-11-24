package com.ushine.cluster.elasticsearch.impl;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.ushine.cluster.elasticsearch.ElasticsearchClient;
import com.ushine.cluster.elasticsearch.IElasticsearchServicve;
import com.ushine.common.utils.StringUtils;

/**
 * IElasticsearchServicve接口实现
 * 
 * @author Franklin
 *
 */
@Service("elasticsearchServicve")
public class ElasticsearchServicveImpl implements IElasticsearchServicve {
	private static final Logger logger = LoggerFactory.getLogger(ElasticsearchServicveImpl.class);
	
	private final boolean isExplain = true;
	
	public boolean createIndex(String indexName) {
		TransportClient client = null;
		try { 
			long startTime = System.currentTimeMillis();
			
			client = ElasticsearchClient.getTransportClient();
			client.admin().indices().prepareCreate(indexName)
				.execute().actionGet();
			
			long stopTime = System.currentTimeMillis();
			logger.info("Elasticsearch新建索引库("+ indexName +")完成.耗时: " 
					+ (stopTime - startTime) + "ms");
			return true;
		} catch(Exception e) {
			logger.error("Elasticsearch新建索引库出错("+ indexName +").", e);
		}
		
		return false;
	}
	
	public boolean putMapping(String indexName, 
			String searchType, List<String[]> mappings) {
		TransportClient client = null;
		try {
			long startTime = System.currentTimeMillis();
			client = ElasticsearchClient.getTransportClient();
			
			XContentBuilder mappingInfo = XContentFactory.jsonBuilder();
			mappingInfo.startObject();
			
			mappingInfo.startObject(indexName).startObject("properties"); // 索引库
			for(String[] doc : mappings) {
				mappingInfo.startObject(doc[0]) // 字段
					.field("type", doc[1])
					.field("store", "yes")
					.field("index", "no_analyzed")
					.endObject();
					
			}
			mappingInfo.endObject().endObject();
			
			mappingInfo.endObject();
			PutMappingRequest mappingRequest = Requests
					.putMappingRequest(indexName).type(searchType).source(mappingInfo);
			client.admin().indices().putMapping(mappingRequest).actionGet();
			
			long stopTime = System.currentTimeMillis();
			logger.info("添加数据映射信息完成. 耗时: " + (stopTime - startTime) + "ms");
			return true;
		} catch (Exception e) {
			logger.error("添加数据映射信息出错.", e);
		}
		
		return false;
	}
	
	public boolean deleteIndex(String indexName) {
		TransportClient client = null;
		try { 
			long startTime = System.currentTimeMillis();
			
			client = ElasticsearchClient.getTransportClient();
			
			DeleteIndexRequest deleetRequest = new DeleteIndexRequest(indexName);
			client.admin().indices().delete(deleetRequest);
			
			long stopTime = System.currentTimeMillis();
			logger.info("Elasticsearch删除索引库("+ indexName +")完成.耗时: " 
					+ (stopTime - startTime) + "ms");
			return true;
		} catch(Exception e) {
			logger.error("Elasticsearch删除索引库出错("+ indexName +").", e);
		}
		
		return false;
	}
	
	public boolean insertData(String indexName, 
			String searchType, String id, Map<String, String> data) {
		TransportClient client = null;
		try {
			long startTime = System.currentTimeMillis();
			client = ElasticsearchClient.getTransportClient();
			
			XContentBuilder document = XContentFactory.jsonBuilder();
			document.startObject();
			Set<Entry<String, String>> dataEntrys = data.entrySet();
			for(Entry<String, String> doc : dataEntrys) {
				document.field(doc.getKey(), doc.getValue());
			}
			document.endObject();
			
			client.prepareIndex(indexName, searchType, id).setSource(document)
				.execute().actionGet();
			
			long stopTime = System.currentTimeMillis();
			logger.info("Elasticsearch(" + indexName + "/" + 
					searchType + ")添加索引完成. 耗时: " + (stopTime - startTime) + "ms");
			return true;
		} catch (Exception e) {
			logger.error("Elasticsearch(" + indexName + "/" + searchType + ")添加索引出错.", e);
		}
		
		return false;
	}
	
	public boolean insertData(String indexName, 
			String searchType, String id, XContentBuilder content) {
		TransportClient client = null;
		try {
			long startTime = System.currentTimeMillis();
			client = ElasticsearchClient.getTransportClient();
			
			client.prepareIndex(indexName, searchType, id).setSource(content)
				.execute().actionGet();
			
			long stopTime = System.currentTimeMillis();
			logger.info("Elasticsearch(" + indexName + "/" + 
					searchType + ")添加索引完成. 耗时: " + (stopTime - startTime) + "ms");
			return true;
		} catch (Exception e) {
			logger.error("Elasticsearch(" + indexName + "/" + searchType + ")添加索引出错.", e);
		}
		
		return false;
	}
	
	public boolean bulkInsertData(String indexName, String searchType,
			Map<String, Map<String, String>> contents) {
		TransportClient client = null;
		try {
			long startTime = System.currentTimeMillis();
			
			client = ElasticsearchClient.getTransportClient();
			BulkRequestBuilder bulkRequest = client.prepareBulk();
			
			Set<Entry<String, Map<String, String>>> contentEntrys = contents.entrySet();
			for(Entry<String, Map<String, String>> content : contentEntrys) {
				XContentBuilder document = XContentFactory.jsonBuilder();
				document.startObject();
				
				Set<Entry<String, String>> dataEntrys = content.getValue().entrySet();
				for(Entry<String, String> data : dataEntrys) {
					document.field(data.getKey(), data.getValue());
				}
					
				document.endObject();
				bulkRequest.add(client.
						prepareIndex(indexName, searchType, content.getKey())
						.setSource(document));
			}
			
			// 提交数据
			BulkResponse bulkResponse = bulkRequest.execute().actionGet();
			if(bulkResponse.hasFailures()) {
				logger.warn("Elasticsearch批量添加索引失败.");
				return false;
			}
			
			long stopTime = System.currentTimeMillis();
			logger.info("Elasticsearch(" + indexName + "/" + 
					searchType + ")批量添加索引完成. 耗时: " + (stopTime - startTime) + "ms");
		} catch(Exception e) {
			logger.error("Elasticsearch(" + indexName + "/" + searchType + ")批量添加索引出错.", e);
		}
		
		return true;
	}
	
	public boolean bulkInsertDocument(String indexName, String searchType,
			Map<String, XContentBuilder> contents) {
		TransportClient client = null;
		try {
			long startTime = System.currentTimeMillis();
			
			client = ElasticsearchClient.getTransportClient();
			BulkRequestBuilder bulkRequest = client.prepareBulk();
			
			Set<Entry<String, XContentBuilder>> contentEntrys = contents.entrySet();
			for(Entry<String, XContentBuilder> content : contentEntrys) {
				bulkRequest.add(client.
						prepareIndex(indexName, searchType, content.getKey())
						.setSource(content.getValue()));
			}
			
			// 提交数据
			BulkResponse bulkResponse = bulkRequest.execute().actionGet();
			if(bulkResponse.hasFailures()) {
				logger.warn("Elasticsearch批量添加索引失败.");
				return false;
			}
			
			long stopTime = System.currentTimeMillis();
			logger.info("Elasticsearch(" + indexName + "/" + 
					searchType + ")批量添加索引完成. 耗时: " + (stopTime - startTime) + "ms");
		} catch(Exception e) {
			logger.error("Elasticsearch(" + indexName + "/" + searchType + ")批量添加索引出错.", e);
		}
		
		return true;
	}
	
	public boolean bulkDeleteData(String indexName, String searchType) {
		TransportClient client = null;
		try { 
			long startTime = System.currentTimeMillis();
			
			client = ElasticsearchClient.getTransportClient();
			MatchAllQueryBuilder allQuery = QueryBuilders.matchAllQuery(); // 所有索引文档
			DeleteByQueryRequestBuilder deleetRequest = client.prepareDeleteByQuery(indexName);
			
			if(searchType != null) {
				// 设置索引类型
				deleetRequest.setTypes(searchType);
			}
			
			deleetRequest.setQuery(allQuery).execute().actionGet();
			
			long stopTime = System.currentTimeMillis();
			logger.info("Elasticsearch删除索引类型("+ indexName + "/" + searchType +")完成.耗时: " 
					+ (stopTime - startTime) + "ms");
			return true;
		} catch(Exception e) {
			logger.error("Elasticsearch删除索引索引类型出错("+ indexName + "/" + searchType +").", e);
		}
		
		return false;
	}
	
	public boolean bulkDeleteData(String indexName, String searchType, String id) {
		TransportClient client = null;
		try { 
			long startTime = System.currentTimeMillis();
			
			client = ElasticsearchClient.getTransportClient();
			client.prepareDelete(indexName, searchType, id).execute().actionGet();
			
			long stopTime = System.currentTimeMillis();
			logger.info("根据索引Id删除Elasticsearch索引数据完成.耗时: " 
					+ (stopTime - startTime) + "ms");
			return true;
		} catch(Exception e) {
			logger.error("根据索引Id删除Elasticsearch索引数据出错.", e);
		}
		
		return false;
	}
	
	public boolean bulkDeleteData(String indexName, String searchType, 
			QueryBuilder query) {
		TransportClient client = null;
		try { 
			long startTime = System.currentTimeMillis();
			
			client = ElasticsearchClient.getTransportClient();
			DeleteByQueryRequestBuilder deleetRequest = client.prepareDeleteByQuery(indexName);
			
			if(searchType != null) {
				// 设置索引类型
				deleetRequest.setTypes(searchType);
			}
			
			deleetRequest.setQuery(query).execute().actionGet();
			
			long stopTime = System.currentTimeMillis();
			logger.info("根据条件批量删除Elasticsearch索引数据完成.耗时: " 
					+ (stopTime - startTime) + "ms");
			return true;
		} catch(Exception e) {
			logger.error("根据条件批量删除Elasticsearch索引数据出错.", e);
		}
		
		return false;
	}

	public SearchHits simpleSearch(String indexName, String[] searchType,
			QueryBuilder query) {
		
		return pagingSearch(indexName, 
				searchType, query, null, null, -1, -1);
	}

	public SearchHits simpleSearch(String indexName, String[] searchType,
			QueryBuilder query, String sortField, String sortType) {
		
		return pagingSearch(indexName, 
				searchType, query, sortField, sortType, -1, -1);
	}

	public SearchHits pagingSearch(String indexName, String[] searchType,
			QueryBuilder query, int start, int size) {
		
		return pagingSearch(indexName, 
				searchType, query, null, null, start, size);
	}

	public SearchHits pagingSearch(String indexName, String[] searchType,
			QueryBuilder query, String sortField,
			String sortType, int start, int size) {
		TransportClient client = null;
		try {
			long startTime = System.currentTimeMillis();
			client = ElasticsearchClient.getTransportClient();
			
			SearchRequestBuilder searchRequest = client.prepareSearch(indexName)
					.setSearchType(SearchType.QUERY_THEN_FETCH)
					.setQuery(query);
			
			if(start>=0 && size>=0) {
				// 设置分页
				searchRequest.setFrom(start).setSize(size);
			}
			
			if(searchType != null) {
				// 设置索引类型
				searchRequest.setTypes(searchType);
			}
			
			if(!StringUtils.isNull(sortField) 
					&& !StringUtils.isNull(sortType)) {
				// 设置字段排序
				if(SortOrder_ASC.equals(sortType)) {
					searchRequest.addSort(sortField, SortOrder.ASC); // 升序
				} else if(SortOrder_DESC.equals(sortType)) {
					searchRequest.addSort(sortField, SortOrder.DESC); // 降序
				}
			}
			
			SearchHits searchHits = searchRequest.execute().actionGet().getHits();
			 
			long stopTime = System.currentTimeMillis();
			logger.info("Elasticsearch检索数据完成(" + searchHits.getTotalHits() + "). sort(" 
					+ sortField + "/" + sortType + "). 耗时: " + (stopTime - startTime) + "ms");
			
			return searchHits;
		} catch (Exception e) {
			logger.error("Elasticsearch检索数据出错.", e);
		}
		
		return null;
	}

	public long getCount(String indexName, String[] searchType,
			QueryBuilder query) {
		TransportClient client = null;
		try {
			long startTime = System.currentTimeMillis();
			client = ElasticsearchClient.getTransportClient();
			
			SearchRequestBuilder searchRequest = client.prepareSearch(indexName)
					.setSearchType(SearchType.COUNT)
					.setQuery(query)
					.setExplain(isExplain);
			
			if(searchType != null) {
				// 没有设置索引类型
				searchRequest.setTypes(searchType);
			}
			
			long totalCount = searchRequest.
					execute().actionGet().getHits().getTotalHits();
			 
			long stopTime = System.currentTimeMillis();
			logger.info("Elasticsearch统计记录总量: " + totalCount + "; 耗时: " 
					+ (stopTime - startTime) + "ms");
			
			return totalCount;
		} catch (Exception e) {
			logger.error("Elasticsearch统计记录总量出错.", e);
		}
		
		return -1;
	}
	
}
