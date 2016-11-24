package com.ushine.eimport.event;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.annotations.GenericGenerator;

/**
 * 数据导入事件类, 记录单文件导入情况
 * 
 * @author Franklin
 *
 */
@Entity
@Table(name = "T_DI_LOGS")
public class DataImportEvent {
	
	/**
	 * 数据导入成功
	 */
	public final static int SUCCEED = 1;
	
	/**
	 * 数据导入失败
	 */
	public final static int FAILURE = -1;
	
	private String id;
	
	private String fileName; // 文件名
	
	private String startTime; // 开始导入时间
	
	private String endTime;   // 导入结束时间
	
	private int totalCount = 0; // 记录总数
	
	private int rightCount = 0; // 正确记录数
	
	private int errorCount = 0; // 错误记录数
	
	private int status; // 本次导入执行状态
	
	public DataImportEvent() {
		
	}
	
	public DataImportEvent(String fileName) {
		this.fileName = fileName;
	}

	public DataImportEvent(String fileName, String startTime, String endTime,
			int totalCount, int rightCount, int errorCount) {
		this.fileName = fileName;
		this.startTime = startTime;
		this.endTime = endTime;
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
	
	@Column(name="FILE_NAME", length=60)
	public String getFileName() {
		return fileName;
	}

	@Column(name="START_DATE", columnDefinition="TIMESTAMP")
	public String getStartTime() {
		return startTime;
	}

	@Column(name="END_DATE", columnDefinition="TIMESTAMP")
	public String getEndTime() {
		return endTime;
	}

	@Column(name="TOTAL_COUNT")
	public int getTotalCount() {
		return totalCount;
	}

	@Column(name="RIGHT_COUNT")
	public int getRightCount() {
		return rightCount;
	}

	@Column(name="ERROR_COUNT")
	public int getErrorCount() {
		return errorCount;
	}

	@Column(name="STATUS")
	public int getStatus() {
		return status;
	}
	
	public void setId(String id) {
		this.id = id;
	}
	
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}

	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}

	public void setTotalCount(int totalCount) {
		this.totalCount = totalCount;
	}

	public void setRightCount(int rightCount) {
		this.rightCount = rightCount;
	}

	public void setErrorCount(int errorCount) {
		this.errorCount = errorCount;
	}
	
	public void setStatus(int status) {
		this.status = status;
	}
	
}
