package org.openmrs.module.bahmnisyncworker;

import java.util.Date;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity(name = "bahmnisyncworker.BahmniSyncWorkerLog")
@Table(name = "bahmnisyncworker_log")
public class BahmniSyncWorkerLog {
	
	public BahmniSyncWorkerLog(){}
	
	public BahmniSyncWorkerLog(Date logDateTime, String message) {
		super();
		this.logDateTime = logDateTime;
		this.message = message;
	}

	public BahmniSyncWorkerLog(Integer bahmniSyncLogId, Date logDateTime, String message) {
		super();
		this.bahmniSyncLogId = bahmniSyncLogId;
		this.logDateTime = logDateTime;
		this.message = message;
	}

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
	
	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}
	
	@Id
	@GeneratedValue
	@Column(name = "bahmnisync_log_id")
	private Integer bahmniSyncLogId;
	
	@Column(name = "log_datetime", nullable = true)
	private Date logDateTime;
	
	@Column(name = "message", nullable = true)
	private String message;
	
	

}
