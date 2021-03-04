package org.openmrs.module.bahmnisyncmaster.service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.hibernate.Criteria;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.LogicalExpression;
import org.hibernate.criterion.Restrictions;
import org.hibernate.jdbc.Work;
import org.openmrs.api.APIException;
import org.openmrs.api.db.hibernate.DbSessionFactory;
import org.openmrs.module.bahmnisyncmaster.BahmniSyncMasterLog;
import org.openmrs.module.bahmnisyncmaster.debezium.DebeziumObject;
import org.openmrs.module.bahmnisyncmaster.util.CommandType;
import org.openmrs.module.bahmnisyncmaster.util.DatabaseUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class DataPullService  {

	@Autowired
	DbSessionFactory sessionFactory;
	
	public BahmniSyncMasterLog saveBahmniSyncMasterLog(BahmniSyncMasterLog log) throws APIException {
		sessionFactory.getCurrentSession().saveOrUpdate(log);
		return log;
	}

	public List<BahmniSyncMasterLog> getConflictBahmniSyncMasterLog() throws APIException {
		Criteria criteria = sessionFactory.getCurrentSession().createCriteria(BahmniSyncMasterLog.class);
		criteria.add(Restrictions.eq("status", "CONFLICT"));
		return criteria.list();
	}
	
	public List<BahmniSyncMasterLog> getAllBahmniSyncMasterLog() throws APIException {
		Criteria criteria = sessionFactory.getCurrentSession().createCriteria(BahmniSyncMasterLog.class);
		return criteria.list();
	}
	
	@Transactional
	public void startDataPush(final Map<String, Object> obj) {
		sessionFactory.getCurrentSession().doWork(new Work() {
			
			public void execute(Connection connection) {
				try {
					DebeziumObject debeziumObject = getDebziumObjectFromMap(obj,connection);
					executeDebeziumObject(debeziumObject, connection);				
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
	}
	
	public DebeziumObject getDebziumObjectFromMap(Map<String, Object> map, Connection con){
		
		ObjectMapper oMapper = new ObjectMapper();
		Map<String, Object> mapData = oMapper.convertValue(map.get("data"), Map.class);
		Map<String, Object> mapPreviousData = oMapper.convertValue(map.get("previous_data"), Map.class);
		ArrayList<Map<String, Object>> fk = (ArrayList<Map<String, Object>>)map.get("fk");
		String query = replacePrimaryKeyTagsInQuery(fk, String.valueOf(map.get("query")), con);

		return new DebeziumObject(String.valueOf(map.get("op")),
														String.valueOf(map.get("db")),
														String.valueOf(map.get("table")),
														mapData, mapPreviousData, fk,
														String.valueOf(map.get("pk")),
														query, String.valueOf(map.get("worker_id")));
				
	}
	
	public String replacePrimaryKeyTagsInQuery (ArrayList<Map<String, Object>> fk, String query, Connection con){
		
		for(Map<String,Object> keys : fk){
			
			String q = "SELECT " + String.valueOf(keys.get("REFERENCED_COLUMN_NAME")) + " from " +
					String.valueOf(keys.get("REFERENCED_TABLE_NAME")) + " where uuid = '" + 
					String.valueOf(keys.get("REFERENCED_UUID")) + "';";
			String qValue = getValue(q,con);
			
			query = query.replace("<"+String.valueOf(keys.get("COLUMN_NAME")) +">", qValue);
			
		}
		
		return query;

	}
	
	public String getValue(String command, Connection con) {
        String str = null;
        try {
            java.sql.Statement st = con.createStatement();
            ResultSet rs = st.executeQuery(command);
            rs.next();
            str = rs.getString(1);
            rs.close();
        } catch (Exception e) {
        	 str = null;
        } 
        return str;
    }
	
	
	public void executeDebeziumObject(DebeziumObject dbObj, Connection con) throws ParseException{
		
		Object ret = null;
			
		if(dbObj.getOp().equals("u")){
			
			String serverData = getServerDataAsString(dbObj,con);
			String clientData = dbObj.getData().toString();
			String clientPreviousData = dbObj.getOldData().toString();
			
			if(serverData.equals(clientData))
				return;
			
			if(!serverData.equals(clientPreviousData)){
			  
	            String dateWorker = (String)dbObj.getData().get("date_changed"); 
				String dateServer = getValue("SELECT date_changed from " + dbObj.getTable() + " where uuid = '" + (String)dbObj.getData().get("uuid")+ "'", con);
				
				Date dateUpdatedWorker = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateWorker);  
				Date dateUpdatedServer = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateServer);
				
			    Date dateVoidedWorker = null;
			    Date dateVoidedServer = null;
			    
			    if(dbObj.getData().containsKey("date_voided") && dbObj.getData().get("date_voided")!= null){
			    
		            String voidedWorker = (String)dbObj.getData().get("date_voided"); 
					String voidedServer = getValue("SELECT date_voided from " + dbObj.getTable() + " where uuid = '" + (String)dbObj.getData().get("uuid")+ "'", con);
					
					dateVoidedWorker = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(voidedWorker);  
					dateVoidedServer = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(voidedServer);  
			    }
			    else if(dbObj.getData().containsKey("date_retired") && dbObj.getData().get("date_retired")!= null){
			    
		            String voidedWorker = (String)dbObj.getData().get("date_retired");
					String voidedServer = getValue("SELECT date_retired from " + dbObj.getTable() + " where uuid = '" + (String)dbObj.getData().get("uuid")+ "'", con);
					 
					dateVoidedWorker = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(voidedWorker);  
					dateVoidedServer = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(voidedServer); 
			    }
			    
			    Date maxServerDate = max(dateUpdatedServer, dateVoidedServer);
			    Date maxClientDate = max(dateUpdatedWorker, dateVoidedWorker);
			    
				Object[][] dataServer = DatabaseUtil.getTableData("SELECT * from " + dbObj.getTable() + " where uuid = '" + (String)dbObj.getData().get("uuid")+"'", con);

			    String s = "";
				Object f[] = dataServer[0];
				for(int i =0; i<f.length; i++)
					s = s + " " + f[i] + ",";
			    
			    if(maxClientDate.after(maxServerDate)){
			    	ret = DatabaseUtil.runCommand(CommandType.UPDATE, dbObj.getQuery(),con);
			    	getLogString(dbObj, "Kept the one form worker due to latest date.", "CONFLICT", con);	    
			    }else{
			    	getLogString(dbObj, "Kept the one form master due to latest date.", "CONFLICT", con);
			    }
			    
			}
			else{
				ret = DatabaseUtil.runCommand(CommandType.UPDATE, dbObj.getQuery(),con);
				if(ret instanceof Exception)
					getLogString(dbObj, ((Exception) ret).getMessage().toString(), "ERROR", con);
				else
					getLogString(dbObj, "", "SUCCESS", con);
			}
			
		}  else if (dbObj.getOp().equals("c")){
			
			String serverData = getServerDataAsString(dbObj,con);
			String clientData = dbObj.getData().toString();
			
			if(serverData.equals(clientData))
				return;
			
			ret = DatabaseUtil.runCommand(CommandType.CREATE, dbObj.getQuery(),con);
			if(ret instanceof Exception)
				getLogString(dbObj, ((Exception) ret).getMessage().toString(), "ERROR", con);
			else
				getLogString(dbObj, "", "SUCCESS", con);
			
		}  else if (dbObj.getOp().equals("d")){
		
			ret = DatabaseUtil.runCommand(CommandType.DELETE, dbObj.getQuery(), con);
			if(ret instanceof Exception)
				getLogString(dbObj, ((Exception) ret).getMessage().toString(), "ERROR", con);
			else
				getLogString(dbObj, "", "SUCCESS", con);
			
		}
	}
	
	public String getServerDataAsString(DebeziumObject dbObject, Connection con){
		
		String columnList = "";
		for ( String key : dbObject.getData().keySet() ) {
			columnList = columnList + key+",";
		}
		columnList = columnList.substring(0,columnList.length()-1);
		
		Object[][] dataServer = DatabaseUtil.getTableData(dbObject.getTable(), columnList, " where uuid = '" + (String)dbObject.getData().get("uuid")+"'", con);

		ArrayList<Map<String,Object>> fkJsonArray = dbObject.getFk();
        		
		StringBuilder serverData = new StringBuilder("{");
		int i = 0;
		String finalData = "";
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
		finalData = serverData.substring(0, serverData.length()-2) + "}";
		}

		return finalData ;
	}
	
	public void getLogString(DebeziumObject dbObject, String message, String status, Connection con){
		
		BahmniSyncMasterLog log = new BahmniSyncMasterLog();
		
		log.setLogDateTime(new Date());
		log.setWorkerId(dbObject.getWorkerId());
		log.setWorkerData(dbObject.getData().toString());
		if(dbObject.getOp().equals("u"))
			log.setMasterData(getServerDataAsString(dbObject, con));
		log.setStatus(status);
		log.setMessage(message);
		log.setTable(dbObject.getTable());
		
		saveBahmniSyncMasterLog(log);
		
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
