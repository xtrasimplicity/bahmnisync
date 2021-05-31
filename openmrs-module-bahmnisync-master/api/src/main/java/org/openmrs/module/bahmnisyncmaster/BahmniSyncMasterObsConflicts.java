package org.openmrs.module.bahmnisyncmaster;

import java.util.Date;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity(name = "bahmnisyncmaster.BahmniSyncMasterObsConflicts")
@Table(name = "bahmnisyncmaster_obs_conflicts")
public class BahmniSyncMasterObsConflicts {
	
	@Id
	@GeneratedValue
	@Column(name = "bahmnisyncmaster_obs_conflicts_id")
	private Integer bahmniSyncObsConflictsId;
	
	@Column(name = "log_datetime", nullable = true)
	private Date logDateTime;

	@Column(name = "worker_id", nullable = true)
	private String workerId;
	
	@Column(name = "worker_data", nullable = true)
	private String workerData;
	
	@Column(name = "value_type", nullable = true)
	private String valueType;
	
	@Column(name = "master_data", nullable = true)
	private String masterData;
	
	@Column(name = "concept_id", nullable = true)
	private Integer conceptId;
	
	@Column(name = "encounter_id", nullable = true)
	private Integer encounterId;
	
	@Column(name = "patient_id", nullable = true)
	private Integer patientId;
	

	public Integer getBahmniSyncObsConflictsId() {
		return bahmniSyncObsConflictsId;
	}

	public void setBahmniSyncObsConflictsId(Integer bahmniSyncObsConflictsId) {
		this.bahmniSyncObsConflictsId = bahmniSyncObsConflictsId;
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
	
	public void setValuetype(String valueType) {
		this.valueType = valueType;
	}
	
	public String getValueType() {
		return valueType;
	}

	public String getMasterData() {
		return masterData;
	}

	public void setMasterData(String masterData) {
		this.masterData = masterData;
	}

	public Integer getConceptId() {
		return conceptId;
	}

	public void setConceptId(Integer conceptId) {
		this.conceptId = conceptId;
	}

	public Integer getEncounterId() {
		return encounterId;
	}

	public void setEncounterId(Integer encounterId) {
		this.encounterId = encounterId;
	}

	public Integer getPatientId() {
		return patientId;
	}

	public void setPatientId(Integer patientId) {
		this.patientId = patientId;
	}
	
}
