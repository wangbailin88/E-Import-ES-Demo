package com.ushine.eimport.relolver.test;

import junit.framework.TestCase;

import org.junit.Test;

import com.ushine.common.utils.StringUtils;
import com.ushine.eimport.relolver.DataModelResolver;

/**
 * 测试数据模型解析器
 * @author franklin
 *
 */
public class DataModelResolverTest extends TestCase {

	@Test
	public void testGetHBaseColumn() {
		DataModelResolver resolver = DataModelResolver.getInstance();
		System.out.println("Junit >> 共配置(" + resolver.size() + ")个字段.");
		
		String[] columns = resolver.getHBaseColumns();
		StringUtils.printArray(columns); // 打印
		String[] indexs = resolver.getIndexFields();
		String[] colTypes = resolver.getColType();
		String[] fNames = resolver.getFieldName();
		for(int i=0; i<columns.length; i++) {
			System.out.println("[" + i + "] c=" + columns[i] + ", i=" + indexs[i] + ", t=" + colTypes[i]);
		}
		
		System.out.println("#////////////////////////////////////////////////////");
		for(int i=0; i<columns.length; i++) {
			if(columns[i].equals("null")) {
				continue;
			}
			System.out.print(columns[i] + " " + colTypes[i] + ",");
		}
		
		System.out.println("#////////////////////////////////////////////////////");
		for(int i=0; i<columns.length; i++) {
			if(columns[i].equals("null")) {
				continue;
			}
			System.out.print("info:" + columns[i] + ",");
		}
		
		System.out.println("#////////////////////////////////////////////////////");
		for(int i=0; i<fNames.length; i++) {
			if(fNames[i].equals("null")) {
				continue;
			}
			System.out.print(fNames[i] + ",");
		}
	}
	
}
