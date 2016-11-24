package com.ushine.cluster.hbase.split;

import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;

@Service("simpleCalculator")
public class SimpleCalculator implements SplitKeysCalculator {

	private static byte[][] splits;
	
	private int regionSize = 10;
	
	@Override
	public void setPartition(int partition) {
		this.regionSize = partition;
	}

	@Override
	public byte[][] calcSplitKeys() {
		if(splits == null) {
			splits = new byte[regionSize][];
			for(int i=0; i<regionSize; i++) {
				splits[i] = Bytes.toBytes(i + "_");
			}
		}
		
		return splits;
	}
	
}
