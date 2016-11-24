package com.ushine.elogs.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.annotations.GenericGenerator;

@Entity
@Table(name = "T_DI_INFO")
public class DICountInfo {

	private String id;
	
	// 记录总数
	private long totalCount = 0; 
	
	// 正确记录数
	private long rightCount = 0; 
	
	// 错误记录数
	private long errorCount = 0; 
		
	public DICountInfo() {
		
	}
	
	public DICountInfo(String id, long totalCount, long rightCount,
			long errorCount) {
		super();
		this.id = id;
		this.totalCount = totalCount;
		this.rightCount = rightCount;
		this.errorCount = errorCount;
	}
	
	@Id
	@GenericGenerator(name="uId", strategy="uuid.hex")
	@GeneratedValue(generator="uId")
	@Column(name="ID", length=32)
	public String getId() {
		return id;
	}
	
	@Column(name="TOTAL_COUNT")
	public long getTotalCount() {
		return totalCount;
	}

	@Column(name="RIGHT_COUNT")
	public long getRightCount() {
		return rightCount;
	}

	@Column(name="ERROR_COUNT")
	public long getErrorCount() {
		return errorCount;
	}
	
	public void setId(String id) {
		this.id = id;
	}

	public void setTotalCount(long totalCount) {
		this.totalCount = totalCount;
	}

	public void setRightCount(long rightCount) {
		this.rightCount = rightCount;
	}

	public void setErrorCount(long errorCount) {
		this.errorCount = errorCount;
	}
	
}
