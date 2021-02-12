package com.ihsinformatics.bahmnisync_server.debezium;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONObject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ihsinformatics.bahmnisync_server.BahmniSyncServer;
import com.ihsinformatics.bahmnisync_server.util.CommandType;

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
	
	public static void executeDebeziumObject(DebeziumObject dbObj) throws ParseException{
		
		Object ret = null;
			
		if(dbObj.getOp().equals("u")){
			
			String serverData = getServerDataAsString(dbObj);
			String clientData = dbObj.getData().toString();
			
			if(serverData.equals(clientData))
				return;
			
			if(dbObj.getData().containsKey("date_changed")){
			  
	            String dateWorker = (String)dbObj.getData().get("date_changed"); 
				String dateServer = BahmniSyncServer.dbUtil.getValue("SELECT date_changed from " + dbObj.getTable() + " where uuid = '" + (String)dbObj.getData().get("uuid")+ "'");
				
				Date dateUpdatedWorker = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateWorker);  
				Date dateUpdatedServer = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateServer);
				
			    Date dateVoidedWorker = null;
			    Date dateVoidedServer = null;
			    
			    if(dbObj.getData().containsKey("date_voided") && dbObj.getData().get("date_voided")!= null){
			    
		            String voidedWorker = (String)dbObj.getData().get("date_voided"); 
					String voidedServer = BahmniSyncServer.dbUtil.getValue("SELECT date_voided from " + dbObj.getTable() + " where uuid = '" + (String)dbObj.getData().get("uuid")+ "'");
					
					dateVoidedWorker = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(voidedWorker);  
					dateVoidedServer = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(voidedServer);  
			    }
			    else if(dbObj.getData().containsKey("date_retired") && dbObj.getData().get("date_retired")!= null){
			    
		            String voidedWorker = (String)dbObj.getData().get("date_retired");
					String voidedServer = BahmniSyncServer.dbUtil.getValue("SELECT date_retired from " + dbObj.getTable() + " where uuid = '" + (String)dbObj.getData().get("uuid")+ "'");
					 
					dateVoidedWorker = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(voidedWorker);  
					dateVoidedServer = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(voidedServer); 
			    }
			    
			    Date maxServerDate = max(dateUpdatedServer, dateVoidedServer);
			    Date maxClientDate = max(dateUpdatedWorker, dateVoidedWorker);
			    
				Object[][] dataServer = BahmniSyncServer.dbUtil.getTableData("SELECT * from " + dbObj.getTable() + " where uuid = '" + (String)dbObj.getData().get("uuid")+"'");

			    String s = "";
				Object f[] = dataServer[0];
				for(int i =0; i<f.length; i++)
					s = s + " " + f[i] + ",";
			    
			    if(maxClientDate.after(maxServerDate)){
			    	ret = BahmniSyncServer.dbUtil.runCommand(CommandType.UPDATE, dbObj.getQuery());
			    	BahmniSyncServer.CONFLICTS_LOGGER.info( getLogString(dbObj) +
									"KEPT THE ONE FROM WORKER DUE TO LATEST DATE!");			    
			    	}else{
			    		
			    		BahmniSyncServer.CONFLICTS_LOGGER.info(getLogString(dbObj) +
										"KEPT THE ONE FROM SERVER DUE TO LATEST DATE!");
			    		
			    	}
			    
			}
			else
				ret = BahmniSyncServer.dbUtil.runCommand(CommandType.UPDATE, dbObj.getQuery());
			
		}  else if (dbObj.getOp().equals("c")){
			
			String serverData = getServerDataAsString(dbObj);
			String clientData = dbObj.getData().toString();
			
			if(serverData.equals(clientData))
				return;
			
			ret = BahmniSyncServer.dbUtil.runCommand(CommandType.CREATE, dbObj.getQuery());
			
		}  else if (dbObj.getOp().equals("d")){
		
			ret = BahmniSyncServer.dbUtil.runCommand(CommandType.DELETE, dbObj.getQuery());
			
		}
		
		if(ret instanceof Exception)
			BahmniSyncServer.ERROR_LOGGER.info(((Exception) ret).getMessage().toString() + " for query -> " + dbObj.getQuery() + "\n");
		
	}
	
	public static String getServerDataAsString(DebeziumObject dbObject){
		
		String columnList = "";
		for ( String key : dbObject.getData().keySet() ) {
			columnList = columnList + key+",";
		}
		columnList = columnList.substring(0,columnList.length()-1);
		
		Object[][] dataServer = BahmniSyncServer.dbUtil.getTableData(dbObject.getTable(), columnList, " where uuid = '" + (String)dbObject.getData().get("uuid")+"'");

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
	
	
}
