package com.ushine.cluster.hbase.split;

/**
 * rowkey生成器
 * 
 * @author Franklin
 *
 */
public interface RowKeyGenerator {

	public byte [] nextId();
	
}
