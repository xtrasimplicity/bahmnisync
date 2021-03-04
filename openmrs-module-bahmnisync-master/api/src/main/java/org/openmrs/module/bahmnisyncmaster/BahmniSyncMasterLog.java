package org.openmrs.module.bahmnisyncmaster;

import java.util.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity(name = "bahmnisyncmaster.BahmniSyncMasterLog")
@Table(name = "bahmnisyncmaster_log")
public class BahmniSyncMasterLog {
	
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
	
	
	@Id
	@GeneratedValue
	@Column(name = "bahmnisync_log_id")
	private Integer bahmniSyncLogId;
	
	@Column(name = "log_datetime", nullable = true)
	private Date logDateTime;

	@Column(name = "worker_id", nullable = true)
	private String workerId;
	
	@Column(name = "worker_data", nullable = true)
	private String workerData;
	
	@Column(name = "master_data", nullable = true)
	private String masterData;
	
	@Column(name = "status", nullable = true)
	private String status;
	
	@Column(name = "message", nullable = true)
	private String message;
	
	@Column(name = "table_name", nullable = true)
	private String table_name;


}
