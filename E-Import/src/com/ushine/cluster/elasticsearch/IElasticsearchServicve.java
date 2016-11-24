package com.ushine.cluster.elasticsearch;

import java.util.List;
import java.util.Map;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHits;


/**
 * Elasticsearch操作接口，对外提供建立索引、查询索引的方法
 * 
 * @author Franklin
 *
 */
public interface IElasticsearchServicve {

	/**
	 * 排序常量: 升序
	 */
	public String SortOrder_ASC = "asc";
	
	/**
	 * 排序常量: 降序
	 */
	public String SortOrder_DESC = "desc";
	
	/**
	 * 创建指定索引库
	 * @param indexName String 索引库名称
	 * @return boolean 成功返回True, 否则返回False
	 */
	public boolean createIndex(String indexName);
	
	/**
	 * 定义索引库德字段名及其数据类型
	 * @param indexName String 索引库
	 * @param searchType String 索引类型
	 * @param mappings List<String[]> Mapping信息
	 * List<String[字段名, 数据类型, store:1=yes|0=no, index:0="not_analyzed"]>
	 * @return boolean 成功返回True, 否则返回False
	 */
	public boolean putMapping(String indexName, 
			String searchType, List<String[]> mappings);
	
	/**
	 * 建立索引数据
	 * @param indexName String 索引库名称
	 * @param searchType String 索引类型
	 * @param id String 数据的ID
	 * @param data Map<String, String> 数据<字段名, 值>
	 * @return boolean 成功返回True, 否则返回False
	 */
	public boolean insertData(String indexName, 
			String searchType, String id, Map<String, String> data);
	
	/**
	 * 建立索引数据
	 * @param indexName String 索引库名称
	 * @param searchType String 索引类型
	 * @param id String 数据的ID
	 * @param content XContentBuilder
	 * @return boolean 成功返回True, 否则返回False
	 */
	public boolean insertData(String indexName, 
			String searchType, String id, XContentBuilder content);
	
	/**
	 * 批量建立索引数据
	 * @param indexName String 索引库名称
	 * @param searchType String 索引类型
	 * @param contents Map<String, Map<String, String>> 
	 * 需要索引的文档集合 数据<id, Map<字段, 值>>
	 * @return boolean 成功返回True, 否则返回False
	 */
	public boolean bulkInsertData(String indexName, 
			String searchType, Map<String, Map<String, String>> contents);
	/**
	 * 批量建立索引数据
	 * @param indexName String 索引库名称
	 * @param searchType String 索引类型
	 * @param contents Map<String, XContentBuilder> 需要索引的文档集合
	 * @return boolean 成功返回True, 否则返回False
	 */
	public boolean bulkInsertDocument(String indexName, 
			String searchType, Map<String, XContentBuilder> contents);
	
	/**
	 * 批量删除索引: 删除指定索引库
	 * @param indexName String 索引库名称
	 * @return boolean 成功返回True, 否则返回False
	 */
	public boolean deleteIndex(String indexName);
	
	/**
	 * 批量删除索引: 删除指定索引类型的索引数据
	 * @param indexName String 索引库名称
	 * @param searchType String 索引类型, 
	 * 	(如果索引类型设置为空则删除全库数据, 但不删除索引库)
	 * @return boolean 成功返回True, 否则返回False
	 */
	public boolean bulkDeleteData(String indexName, String searchType);
			
	/**
	 * 批量删除索引: 根据ID删除指定索引
	 * @param indexName String 索引库名称
	 * @param searchType String 索引类型(一个索引库有多个索引类型) 
	 * @Param id String 索引ID
	 * @return boolean 成功返回True, 否则返回False
	 */
	public boolean bulkDeleteData(String indexName, String searchType, String id);
	
	/**
	 * 批量删除索引: 删除符合条件的索引数据 
	 * @param indexName String 索引库名称
	 * @param searchType String 索引类型(一个索引库有多个索引类型) 
	 * @param query QueryBuilder 查询条件
	 * @return boolean 成功返回True, 否则返回False
	 */
	public boolean bulkDeleteData(String indexName, String searchType, 
			QueryBuilder query);
	
	/**
	 * 简单数据检索: 不采用不分页机制, 数据数量过大会造成内存溢出
	 * @param indexName String 索引库名称
	 * @param searchType String 索引类型(一个索引库有多个索引类型) 
	 * @param query QueryBuilder 查询条件
	 * @return SearchHits 查询到的结果集合, 出现异常返回null
	 */
	public SearchHits simpleSearch(String indexName, String[] searchType, 
			QueryBuilder termQuery);
	
	/**
	 * 排序功能的简单数据检索: 不采用不分页机制, 数据数量过大会造成内存溢出
	 * @param indexName String 索引库名称
	 * @param searchType String 索引类型(一个索引库有多个索引类型) 
	 * @param query QueryBuilder 查询条件
	 * @param sortField String 排序字段名称
	 * @param sortType String 排序方式(asc, desc)
	 * @return SearchHits 查询到的结果集合, 出现异常返回null
	 */
	public SearchHits simpleSearch(String indexName, String[] searchType, 
			QueryBuilder query, String sortField, String sortType);
	
	/**
	 * 分页检索数据
	 * @param indexName String 索引库名称
	 * @param searchType String 索引类型(一个索引库有多个索引类型) 
	 * @param query QueryBuilder 查询条件
	 * @param start int 分页查询的起始记录
	 * @param size int 分页查询每页返回的记录数量
	 * @return SearchHits 查询到的结果集合, 出现异常返回null
	 */
	public SearchHits pagingSearch(String indexName, 
			String[] searchType, QueryBuilder query, int start, int size);
	
	/**
	 * 排序功能的分页检索数据
	 * @param ndexName String 索引库名称
	 * @param searchType String 索引类型(一个索引库有多个索引类型) 
	 * @param query QueryBuilder 查询条件
	 * @param sortField String 排序字段名称
	 * @param sortType String 排序方式(asc, desc)
	 * @param start int 分页查询的起始记录
	 * @param size int 分页查询每页返回的记录数量
	 * @return SearchHits 查询到的结果集合, 出现异常返回null
	 */
	public SearchHits pagingSearch(String indexName, String[] searchType, 
			QueryBuilder termQuery, String sortField, String sortType, 
			int start, int size);
	
	/**
	 * 获取数据总量
	 * @param ndexName String 索引库名称
	 * @param searchType String 索引类型(一个索引库有多个索引类型) 
	 * @param termQuery QueryBuilder 查询条件
	 * @return long 查询到的数据总量, 出现异常返回 -1
	 */
	public long getCount(String indexName, String[] searchType, 
			QueryBuilder query);
	
}
