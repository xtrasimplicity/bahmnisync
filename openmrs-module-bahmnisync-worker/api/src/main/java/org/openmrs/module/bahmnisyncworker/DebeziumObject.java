package org.openmrs.module.bahmnisyncworker;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Map;

import org.json.JSONObject;

public class DebeziumObject {

	private String op;
	private String db;
	private String table;
	private Map<String, Object> data;
	private ArrayList<Map<String, Object>> fk;
	private String pk; 
	private String query;

	public DebeziumObject(String op, String db, String table,  Map<String, Object> data, 
			ArrayList<Map<String, Object>> fk, String pk, String query) {
		this.op = op;
		this.db = db;
		this.table = table;
		this.data = data;
		this.fk = fk;
		this.pk = pk;
		this.query = query;
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
	
	public ArrayList<Map<String, Object>> getFk() {
		return fk;
	}
	
	public void setData(String colName, String value){
		data.put(colName, value);
	}

	public String getPk() {
		return pk;
	}
	
	public String getQuery() {
		return query;
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

	public void setFk(ArrayList<Map<String, Object>> fk) {
		this.fk = fk;
	}

	public void setPk(String pk) {
		this.pk = pk;
	}

	public void setQuery(String query) {
		this.query = query;
	}
	
}

