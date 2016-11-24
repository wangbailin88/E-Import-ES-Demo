package com.ushine.cluster.hbase.split;

/**
 * HBase预建分区接口
 * 
 * @author Franklin
 *
 */
public interface SplitKeysCalculator {

	/**
	 * 获取SplitKeys
	 * @return byte[][]
	 */
	public byte[][] calcSplitKeys();
	
	/**
	 * 设置分区数量
	 * @param partition int 
	 */
	public void setPartition(int partition);
	
}
