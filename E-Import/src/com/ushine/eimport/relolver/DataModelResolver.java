package com.ushine.eimport.relolver;

import java.util.List;

import org.dom4j.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ushine.common.utils.PathUtils;
import com.ushine.common.utils.XMLUtils;

/**
 * 数据模型解析器，提供对"WEB-INF/data-model.xml"文件的解功
 * 能，识别出CSV文件中字段的存储顺序以及字段的索引规则，根据
 * 该模型规则进行数据入库及建立索引操作。
 * 
 * @author franklin
 *
 */
public class DataModelResolver {
	private static final Logger logger = LoggerFactory.getLogger(DataModelResolver.class);
	
	public static final String DATA_MODEL_FILE = "data-model.xml";
	
	public static final String DATA_MODEL_TAG_FIELD = "field";
	
	public static final String DATA_MODEL_TAG_COLUMN = "column";
	
	public static final String DATA_MODEL_TAG_INDEX = "index";
	
	public static final String DATA_MODEL_TAG_TYPE = "type";
	
	public static final String DATA_MODEL_TAG_META = "meta";
	
	public static DataModelResolver resolver;
	
	private String[] hBaseColumns = null;
	
	private String[] indexFields = null;
	
	private String[] cloTypes = null;
	
	private String[] fieldName = null;
	
	private DataModelResolver() {
		try {
			String filePath = PathUtils.getConfigPath(DataModelResolver.class) + DATA_MODEL_FILE;
			XMLUtils xml = new XMLUtils(filePath);
			List<Element> columnEles = xml.getNodes(DATA_MODEL_TAG_FIELD);
			hBaseColumns = new String[columnEles.size()];
			indexFields = new String[columnEles.size()];
			cloTypes = new String[columnEles.size()];
			fieldName = new String[columnEles.size()];
			
			// 循环配置文件的元素，按读取顺序保存到数组
			int i=0;
			for(Element columnEle : columnEles) {
				hBaseColumns[i] = xml.getNodeAttrVal(columnEle, DATA_MODEL_TAG_COLUMN);
				indexFields[i] = xml.getNodeAttrVal(columnEle, DATA_MODEL_TAG_INDEX);
				cloTypes[i] = xml.getNodeAttrVal(columnEle, DATA_MODEL_TAG_TYPE);
				fieldName[i] = xml.getNodeAttrVal(columnEle, DATA_MODEL_TAG_META);
				i++;
			}
			
			logger.debug("成功加载数据模型文件:" + filePath + ".");
		} catch(Exception e) {
			logger.error("数据模型文件加载失败:" + DATA_MODEL_FILE + ",关闭应用程序.", e);
			System.exit(-1);
		}
	}
	
	/**
	 * 获取HBase列信息，根据该名称及顺序入库
	 * @return String[] 列信息
	 */
	public String[] getHBaseColumns() {
		
		return hBaseColumns;
	}
	
	/**
	 * 获取Index信息，根据该名称及顺序入库
	 * @return String[] 索引信息，null不进行入库
	 */
	public String[] getIndexFields() {
		
		return indexFields;
	}
	
	/**
	 * 获取字段类型，根据该名称及顺序入库
	 * @return String[] null不进行入库
	 */
	public String[] getColType() {
		
		return cloTypes;
	}
	
	public String[] getFieldName() {
		
		return fieldName;
	}
	
	/**
	 * 统计配置信息中字段的数量
	 * @return int
	 */
	public int size() {
		
		return hBaseColumns.length;
	}
	
	/**
	 * 获取数据模型解析器
	 * @return DataModelResolver
	 */
	public static DataModelResolver getInstance() {
		if(resolver == null) {
			resolver = new DataModelResolver();
		}
		logger.debug("获取数据模型解析器("+resolver.toString()+").");
		return resolver;
	}
	
}
