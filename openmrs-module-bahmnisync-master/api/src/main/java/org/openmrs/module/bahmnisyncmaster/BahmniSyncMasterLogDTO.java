package org.openmrs.module.bahmnisyncmaster;

import java.util.Date;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;


public class BahmniSyncMasterLogDTO {
	
	public Integer getBahmniSyncLogId() {
		return bahmniSyncLogId;
	}

	public void setBahmniSyncLogId(Integer bahmniSyncLogId) {
		this.bahmniSyncLogId = bahmniSyncLogId;
	}

	public Date getLogDateTime() {
		return logDateTime;
	}

	public void setLogDateTime(java.util.Date date) {
		this.logDateTime = date;
	}

	public String getWorkerId() {
		return workerId;
	}

	public void setWorkerId(String workerId) {
		this.workerId = workerId;
	}

	public String getWorkerData() {
		return workerData;
	}

	public void setWorkerData(String workerData) {
		this.workerData = workerData;
	}

	public String getMasterData() {
		return masterData;
	}

	public void setMasterData(String masterData) {
		this.masterData = masterData;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}
	
	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}
	
	public String getTable() {
		return table_name;
	}

	public void setTable(String table) {
		this.table_name = table;
	}
	
	private Integer bahmniSyncLogId;
	
	private Date logDateTime;

	private String workerId;
	
	private String workerData;
	
	private String masterData;
	
	private String status;
	
	private String message;
	
	private String table_name;

}
