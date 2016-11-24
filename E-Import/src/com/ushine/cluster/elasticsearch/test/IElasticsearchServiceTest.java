package com.ushine.cluster.elasticsearch.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.ushine.cluster.elasticsearch.IElasticsearchServicve;

/**
 * Elasticsearch接口操作示例及测试
 * 
 * @author Franklin
 *
 */
@Component("elasticsearchServiceTest")
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "/applicationContext.xml")
public class IElasticsearchServiceTest extends TestCase {
	
	private final String indexName = "test_tmp_table"; // 索引名称
	
	private final String searchType[] = 
		{"test_st_0", "test_st_1",  "test_st_2", "test_st_3", "test_st_4"};
	
	private final String searchTypeStr = null;
	
	private final String sortField = "字段0"; // 排序字段
	
	private final String sortType = IElasticsearchServicve.SortOrder_ASC; // 排序方式
	
	QueryBuilder termQuery = QueryBuilders.
			queryString("\"值100\"").field("字段0"); // 查询条件
	
	FilterBuilder filter = null; // 过滤条件
	
	@Autowired
	private IElasticsearchServicve servicve;
	
	/*
	 * 建立索引库
	 */
	@Test
	public void testCreateIndex() {
		try {
			if(servicve.createIndex(indexName)) {
				System.out.println("Create Index Succeed.");
			} else {
				System.out.println("Create Index  Error !!!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * 建立索引库映射信息
	 */
	@Test
	public void testPutMapping() {
		try {
			String searchType = "info";
			List<String[]> mappings = new ArrayList<String[]>();
			mappings.add(new String[]{"name", "String"});
			mappings.add(new String[]{"mobile", "String"});
			mappings.add(new String[]{"resultCount", "String"});
			
			if(servicve.putMapping(indexName, searchType, mappings)) {
				System.out.println("Create Index Succeed.");
			} else {
				System.out.println("Create Index  Error !!!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * 批量添加索引 
	 */
	@Test
	public void testInsertData() {
		try {
			String indexName = "test_tmp_table";
			String searchType = "info";
			System.out.println("IndexName=" + indexName + "| SearchType=" +searchType);
			
			// 准备数据
			Map<String, String> data = new HashMap<String, String>();
			
			for(int j=0; j<10; j++) {
				data.put("字段"+j, "值"+j);
			}
			
			if(servicve.insertData(indexName, searchType, "testRowKey", data)) {
				System.out.println("Test Insert Data Succeed.");
			} else {
				System.out.println("Test Insert Data Error !!!");
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * 批量添加索引 
	 */
	@Test
	public void testBulkInsertData() {
		try {
			for(int st=0; st<10; st++) {
				String indexName = "test_tmp_table";
				String searchType = "test_st_" + st;
				System.out.println("IndexName=" + indexName + "| SearchType=" +searchType);
				
				// 准备数据
				Map<String, Map<String, String>> contents = new HashMap<String, Map<String, String>>();
				for(int i=0; i<10000; i++) {
					Map<String, String> data = new HashMap<String, String>();
					
					for(int j=0; j<2; j++) {
						data.put("字段"+j, "值"+i+j);
					}
					
					
					contents.put(Integer.toString(i), data);
				}
				
				if(servicve.bulkInsertData(indexName, searchType, contents)) {
					System.out.println("Test Bulk Insert Data Succeed.");
				} else {
					System.out.println("Test Bulk Insert Data Error !!!");
				}
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * 批量添加索引 
	 */
	@Test
	public void testBulkInsertDocument() {
		try {
			for(int st=0; st<10; st++) {
				String indexName = "test_tmp_table";
				String searchType = "test_st_" + st;
				System.out.println("IndexName=" + indexName + "| SearchType=" +searchType);
				
				// 准备数据
				Map<String, XContentBuilder> contents = new HashMap<String, XContentBuilder>();
				for(int i=0; i<10000; i++) {
					XContentBuilder document = XContentFactory.jsonBuilder();
					document.startObject();
					
					for(int j=0; j<100; j++) {
						document.field("字段"+j, "值"+i+j);
					}
					
					document.endObject();	
					contents.put(Integer.toString(i), document);
				}
				
				if(servicve.bulkInsertDocument(indexName, searchType, contents)) {
					System.out.println("Test Bulk Insert Data Succeed.");
				} else {
					System.out.println("Test Bulk Insert Data Error !!!");
				}
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * 根据ID删除索引
	 */
	@Test
	public void testBulkDeleteDataById() {
		String id = "0";
		String searchTypeStr = "test_st_1";
		
		if(servicve.bulkDeleteData(indexName, searchTypeStr, id)) {
			System.out.println("Test Delete Data By Id Succeed.");
		} else {
			System.out.println("Test Delete Data By Id Error !!!");
		}
	}
	
	/*
	 * 根据条件删除索引
	 */
	@Test
	public void testBulkDeleteDataByQuery() {
		if(servicve.bulkDeleteData(indexName, searchTypeStr, termQuery)) {
			System.out.println("Test Delete Search Type Succeed.");
		} else {
			System.out.println("Test Delete Search Type Error !!!");
		}
	}
	
	/*
	 * 删除指定索引类型
	 */
	@Test
	public void testBulkDeleteSearchType() {
		if(servicve.bulkDeleteData(indexName, searchTypeStr)) {
			System.out.println("Test Delete Data By Id Succeed.");
		} else {
			System.out.println("Test Delete Data By Id Error !!!");
		}
	}
	
	/*
	 * 删除索引库
	 */
	@Test
	public void testDeleteIndex() {
		if(servicve.deleteIndex(indexName)) {
			System.out.println("Test Delete Index Succeed.");
		} else {
			System.out.println("Test Delete Index Error !!!");
		}
	}
	
	/*
	 * 简单检索不排序
	 */
	@Test
	public void testSimpleSearch() {
		SearchHits hits = servicve.simpleSearch(indexName, searchType, termQuery);
		if(hits != null) {
			System.out.println("Test Search Data Not Sort: " + hits.getTotalHits()); 
			for(SearchHit hit : hits) {
				System.out.println(hit.getSourceAsString());
			}
		} else {
			System.out.println("Test Search Data Error !!!"); 
		}
	}
	
	/*
	 * 分页检索并排序 (测试翻页)
	 */
	@Test
	public void testPagingSearch() {
		int start = 0;
		int size = 10;
		
		while(start < 50) {
			SearchHits hits = servicve.pagingSearch(indexName, 
					searchType, termQuery, sortField, sortType, start, size);
			if(hits != null) {
				System.out.println("Test Search Data Count: " + hits.getTotalHits()); 
				for(SearchHit hit : hits) {
					System.out.println(hit.getSourceAsString());
				}
			} else {
				System.out.println("Test Search Data Error !!!");
				break;
			}
			
			start += 10;
		}
	}
	
	/*
	 * 统计数据总量
	 */
	@Test
	public void testGetCount() {
		long total = servicve.getCount(indexName, searchType, termQuery);
		if(total > -1) {
			System.out.println("Test Get Count Data Total: " + total);
		} else {
			System.out.println("Test Get Count Data Total Error !!!");
		}
	}
	
	public IElasticsearchServicve getServicve() {
		return servicve;
	}
	
	public void setServicve(IElasticsearchServicve servicve) {
		this.servicve = servicve;
	}
	
}
