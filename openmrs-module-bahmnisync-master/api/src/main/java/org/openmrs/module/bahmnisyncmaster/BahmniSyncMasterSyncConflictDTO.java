package org.openmrs.module.bahmnisyncmaster;

import java.util.Date;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;


public class BahmniSyncMasterSyncConflictDTO {

		
	public Integer getBahmniSyncObsConflictId() {
		return bahmniSyncObsConflictId;
	}

	public void setBahmniSyncObsConflictId(Integer bahmniSyncObsConflictId) {
		this.bahmniSyncObsConflictId = bahmniSyncObsConflictId;
	}

	public Date getLogDateTime() {
		return logDateTime;
	}

	public void setLogDateTime(Date logDateTime) {
		this.logDateTime = logDateTime;
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

	public String getConcept() {
		return concept;
	}

	public void setConcept(String concept) {
		this.concept = concept;
	}

	public String getEncounter() {
		return encounter;
	}

	public void setEncounter(String encounter) {
		this.encounter = encounter;
	}

	public String getPatient() {
		return patient;
	}

	public void setPatient(String patient) {
		this.patient = patient;
	}

	private Integer bahmniSyncObsConflictId;

	private Date logDateTime;

	private String workerId;
	
	private String workerData;
	
	private String masterData;
	
	private String concept;
	
	private String encounter;
	
	private String patient;

}
