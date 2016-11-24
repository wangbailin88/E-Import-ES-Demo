package com.ushine.cluster.hbase.split;

import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.ushine.common.utils.RandomUtils;

/**
 * 按Hash方式预建分区实现
 * 以后在插入数据的时候，也要按照此rowkeyGenerator的方式生成rowkey
 * 
 * @author Franklin
 *
 */
@Service("hashChoreWoker")
public class HashChoreWoker implements SplitKeysCalculator {
	private static final Logger logger = LoggerFactory.getLogger(HashChoreWoker.class);
	
	// 随机取机数目 
	private int baseRecord = RandomUtils.getRandomInt(1000000);
	
	// splitkeys个数 
	private int splitKeysNumber;
	
	// rowkey生成器 
	private RowKeyGenerator rkGen;
	
	// 取样时，由取样数目及region数相除所得的数量. 
	private int splitKeysBase;
	
	// 由抽样计算出来的splitkeys结果 
	private byte[][] splitKeys;
	
	private int prepareRegions;
	
	public HashChoreWoker() {
		// 实例化rowkey生成器 
		rkGen = new HashRowKeyGenerator();
	}
	
	public void setPartition(int partition) {
		prepareRegions = partition;
		splitKeysNumber = prepareRegions - 1;
		splitKeysBase = baseRecord / prepareRegions;
	}
	
	public byte[][] calcSplitKeys() {
		logger.info("由抽样计算出来的splitkeys结果.....(" + baseRecord + ")");
		splitKeys = new byte[splitKeysNumber][];
		
		//使用TreeSet保存抽样数据，已排序过 
		TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR); 
		for(int i = 0; i < baseRecord; i++) {
			rows.add(rkGen.nextId());
		}
		
		int pointer = 0;
		int index = 0;
		Iterator<byte[]> rowKeyIter = rows.iterator();
		while (rowKeyIter.hasNext()) { 
			byte[] tempRow = rowKeyIter.next();
			rowKeyIter.remove();
			
			if((pointer!=0) && (pointer%splitKeysBase==0)) {
				if(index < splitKeysNumber) {
					splitKeys[index] = tempRow;
					index ++;
				}
			}
			pointer ++;
		}
		
		rows.clear();
		rows = null;
		return splitKeys;
	}
	
}