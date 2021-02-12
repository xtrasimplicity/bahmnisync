package com.ihsinformatics.bahmnisyncclient.debezium;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONObject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ihsinformatics.bahmnisyncclient.BahmniSyncClient;
import com.ihsinformatics.bahmnisyncclient.util.CommandType;

public class DebeziumService {
	
	public static DebeziumObject getDebziumObjectFromMap(Map<String, Object> map){
		
		ObjectMapper oMapper = new ObjectMapper();
		Map<String, Object> mapData = oMapper.convertValue(map.get("data"), Map.class);
		ArrayList<Map<String, Object>> fk = (ArrayList<Map<String, Object>>)map.get("fk");
				
		DebeziumObject debeziumObject = new DebeziumObject(String.valueOf(map.get("op")),
														String.valueOf(map.get("db")),
														String.valueOf(map.get("table")),
														mapData,fk,
														String.valueOf(map.get("pk")),
														String.valueOf(map.get("query")));
		
		debeziumObject.replacePrimaryKeyTagsInQuery();
		
		return debeziumObject;
		
	}
	
	public static DebeziumObject getDebziumObjectFromJSON(JSONObject map){
		
		ObjectMapper oMapper = new ObjectMapper();
		Map<String, Object> mapData = toMap(map.getJSONObject("data"));
		JSONArray fkArray = map.getJSONArray("fk");
		ArrayList<Map<String,Object>> fk = new ArrayList<Map<String,Object>>();     
		if (fkArray != null) { 
		   for (int i=0;i<fkArray.length();i++){ 
			  fk.add(toMap(fkArray.getJSONObject(i)));
		   } 
		} 
						
		DebeziumObject debeziumObject = new DebeziumObject(map.getString("op"),
														map.getString("db"),
														map.getString("table"),
														mapData,fk,
														map.getString("pk"),
														map.getString("query"));
		
		debeziumObject.replacePrimaryKeyTagsInQuery();
		
		return debeziumObject;
		
	}
	
	public static void executeDebeziumObject(DebeziumObject dbObj) throws ParseException{
		
		Object ret = null;
				
		System.out.println(dbObj.getQuery());
		
		if(dbObj.getOp().equals("u")){
			
			String serverData = getServerDataAsString(dbObj);
			String clientData = dbObj.getData().toString();
			
			if(serverData.equals(clientData))
				return;
			
			ret = BahmniSyncClient.dbUtil.runCommand(CommandType.UPDATE, dbObj.getQuery());
			
		}  else if (dbObj.getOp().equals("c")){
			
			String serverData = getServerDataAsString(dbObj);
			String clientData = dbObj.getData().toString();
			
			if(serverData.equals(clientData))
				return;
			
			ret = BahmniSyncClient.dbUtil.runCommand(CommandType.CREATE, dbObj.getQuery());
			
		}  else if (dbObj.getOp().equals("d")){
		
			ret = BahmniSyncClient.dbUtil.runCommand(CommandType.DELETE, dbObj.getQuery());
			
		}
		
	}
	
	public static String getServerDataAsString(DebeziumObject dbObject){
		
		String columnList = "";
		for ( String key : dbObject.getData().keySet() ) {
			columnList = columnList + key+",";
		}
		columnList = columnList.substring(0,columnList.length()-1);
		
		Object[][] dataServer = BahmniSyncClient.dbUtil.getTableData(dbObject.getTable(), columnList, " where uuid = '" + (String)dbObject.getData().get("uuid")+"'");

		ArrayList<Map<String,Object>> fkJsonArray = dbObject.getFk();
        		
		StringBuilder serverData = new StringBuilder("{");
		int i = 0;
		if(dataServer != null && dataServer.length != 0){
		for(String key : dbObject.getData().keySet() ){
			
				if(key.equals(dbObject.getPk()) && dataServer[0][i] instanceof Integer){
					serverData.append(key + "=PK, ");
				}else {
					
					if(isForeignKey(key,fkJsonArray)){
						
						for(int j=0; j<fkJsonArray.size(); j++){
							Map<String,Object> jObject = fkJsonArray.get(j);
							if(jObject.get("COLUMN_NAME").equals(key))
								serverData.append(key + "=" + jObject.get("REFERENCED_UUID") + ", ");
						}
						
					}
					else{
						
						if(dataServer[0][i] instanceof java.sql.Timestamp )
							dataServer[0][i] = String.valueOf(dataServer[0][i]).substring(0, String.valueOf(dataServer[0][i]).length()-2);
						
						serverData.append(key + "=" + dataServer[0][i] + ", ");
					}
				}
				i++;
			}
		serverData.append(serverData.toString().substring(0, serverData.length()-2));
		}
		return serverData + "}";
	}
	
	public static String getLogString(DebeziumObject dbObject){
		
		return "\nFROM WORKER -> " + dbObject.getData() + "\n" + "FROM SERVER -> " + getServerDataAsString(dbObject) + "\n";
	
	}
	
	
	public static Boolean isForeignKey(String colName, ArrayList<Map<String,Object>> fkJsonArray){
		
		for(int i=0; i<fkJsonArray.size(); i++){
			Map<String,Object> jObject = fkJsonArray.get(i);
			if(jObject.get("COLUMN_NAME").equals(colName))
				return true;
		}
		
		return false;
	}
	
	public static Date max(Date d1, Date d2) {
        if (d1 == null && d2 == null) return null;
        if (d1 == null) return d2;
        if (d2 == null) return d1;
        return (d1.after(d2)) ? d1 : d2;
    }
	
	public static Map<String, Object> toMap(JSONObject jb) {
	    Map<String, Object> results = new HashMap<String, Object>();
	    Iterator<String> keys = jb.keys();
	    
	    while(keys.hasNext()) {
	    	Object value = null;
	        String key = keys.next();
	        value =  jb.get(key);
	        results.put(key, value); 
	    }
	    return results;
	}
	    
	    
	
}
