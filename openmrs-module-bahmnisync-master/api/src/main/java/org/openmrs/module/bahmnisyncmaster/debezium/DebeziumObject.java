package org.openmrs.module.bahmnisyncmaster.debezium;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Map;

public class DebeziumObject {

	private String op;
	private String db;
	private String table;
	private Map<String, Object> data;
	private Map<String, Object> previousData;
	private ArrayList<Map<String, Object>> fk;
	private String pk; 
	private String query;
	private String workerId;

	public DebeziumObject(String op, String db, String table,  Map<String, Object> data,  Map<String, Object> previousData,
			ArrayList<Map<String, Object>> fk, String pk, String query, String workerId) {
		this.op = op;
		this.db = db;
		this.table = table;
		this.data = data;
		this.previousData = previousData;
		this.fk = fk;
		this.pk = pk;
		this.query = query;
		this.workerId = workerId;
	}

	public String getOp() {
		return op;
	}

	public String getDb() {
		return db;
	}
	
	public String getTable() {
		return table;
	}

	public Map<String, Object> getData() {
		return data;
	}
	
	public Map<String, Object> getOldData() {
		return previousData;
	}
	
	public ArrayList<Map<String, Object>> getFk() {
		return fk;
	}
	
	public void setData(String colName, String value){
		data.put(colName,value);
	}

	public String getPk() {
		return pk;
	}
	
	public String getQuery() {
		return query;
	}
	
	public String getWorkerId() {
		return workerId;
	}
	
	public void setOp(String op) {
		this.op = op;
	}

	public void setDb(String db) {
		this.db = db;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public void setData(Map<String, Object> data) {
		this.data = data;
	}
	
	public void setOldData(Map<String, Object> data) {
		this.previousData = data;
	}

	public void setFk(ArrayList<Map<String, Object>> fk) {
		this.fk = fk;
	}

	public void setPk(String pk) {
		this.pk = pk;
	}

	public void setQuery(String query) {
		this.query = query;
	}
	
	public void setWorkerId(String workerId) {
		this.workerId = workerId;
	}
}
