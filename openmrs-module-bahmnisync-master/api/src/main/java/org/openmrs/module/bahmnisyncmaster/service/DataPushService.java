package org.openmrs.module.bahmnisyncmaster.service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.codehaus.jackson.map.ObjectMapper;
import org.hibernate.Criteria;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.LogicalExpression;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Restrictions;
import org.hibernate.jdbc.Work;
import org.openmrs.Concept;
import org.openmrs.Encounter;
import org.openmrs.Obs;
import org.openmrs.Patient;
import org.openmrs.Person;
import org.openmrs.User;
import org.openmrs.annotation.Authorized;
import org.openmrs.api.APIException;
import org.openmrs.api.context.Context;
import org.openmrs.api.db.hibernate.DbSessionFactory;
import org.openmrs.module.bahmnisyncmaster.BahmniSyncMasterLog;
import org.openmrs.module.bahmnisyncmaster.BahmniSyncMasterLogDTO;
import org.openmrs.module.bahmnisyncmaster.BahmniSyncMasterObsConflicts;
import org.openmrs.module.bahmnisyncmaster.debezium.DebeziumObject;
import org.openmrs.module.bahmnisyncmaster.util.CommandType;
import org.openmrs.module.bahmnisyncmaster.util.DatabaseUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.openmrs.module.bahmnisyncmaster.util.BahmniSyncMasterConstants;

@Service
public class DataPushService  {

	@Autowired
	DbSessionFactory sessionFactory;
	
	public BahmniSyncMasterLog getBahmniSyncMasterLogById(Integer id) {
		Criteria criteria = sessionFactory.getCurrentSession().createCriteria(BahmniSyncMasterLog.class);
		criteria.add(Restrictions.eq("bahmniSyncLogId", id));
		return (BahmniSyncMasterLog) criteria.uniqueResult();
	}
	
	@Authorized(BahmniSyncMasterConstants.MANAGE_BAHMNI_SYNC_PRIVILEGE)
	public BahmniSyncMasterObsConflicts getBahmniSyncMasterObsConflictById(Integer id){
		Criteria criteria = sessionFactory.getCurrentSession().createCriteria(BahmniSyncMasterObsConflicts.class);
		criteria.add(Restrictions.eq("bahmniSyncObsConflictsId", id));
		return (BahmniSyncMasterObsConflicts) criteria.uniqueResult();
	}
	
	@Authorized(BahmniSyncMasterConstants.MANAGE_BAHMNI_SYNC_PRIVILEGE)
	public BahmniSyncMasterLog saveBahmniSyncMasterLog(BahmniSyncMasterLog log) throws APIException {
		sessionFactory.getCurrentSession().saveOrUpdate(log);
		return log;
	}
	
	@Authorized(BahmniSyncMasterConstants.MANAGE_BAHMNI_SYNC_PRIVILEGE)
	public BahmniSyncMasterObsConflicts saveBahmniSyncMasterObsConflict(BahmniSyncMasterObsConflicts log) throws APIException {
		sessionFactory.getCurrentSession().saveOrUpdate(log);
		return log;
	}


	@Authorized(BahmniSyncMasterConstants.MANAGE_BAHMNI_SYNC_PRIVILEGE)
	public List<BahmniSyncMasterLog> getConflictBahmniSyncMasterLog() throws APIException {		
		Criteria criteria = sessionFactory.getCurrentSession().createCriteria(BahmniSyncMasterLog.class);
		criteria.add(Restrictions.eq("status", "CONFLICT"));
		criteria.addOrder(Order.desc("logDateTime"));
		return criteria.list();
	}
	
	@Authorized(BahmniSyncMasterConstants.MANAGE_BAHMNI_SYNC_PRIVILEGE)
	public List<BahmniSyncMasterObsConflicts> getObsConflictBahmniSyncMasterLog() throws APIException {		
		Criteria criteria = sessionFactory.getCurrentSession().createCriteria(BahmniSyncMasterObsConflicts.class);
		return criteria.list();
	}
	
	@Authorized(BahmniSyncMasterConstants.MANAGE_BAHMNI_SYNC_PRIVILEGE)
	@Transactional
	public void markLogAsResolved(Integer id) throws APIException {
		BahmniSyncMasterLog log = getBahmniSyncMasterLogById(id);
		log.setStatus("CONFLICT RESOLVED");
		sessionFactory.getCurrentSession().update(log);
	}
	
	@Authorized(BahmniSyncMasterConstants.MANAGE_BAHMNI_SYNC_PRIVILEGE)
	@Transactional
	public void deleteObsConflict(Integer id) throws APIException {
		BahmniSyncMasterObsConflicts log = getBahmniSyncMasterObsConflictById(id);
		sessionFactory.getCurrentSession().delete(log);
	}
	
	@Authorized(BahmniSyncMasterConstants.MANAGE_BAHMNI_SYNC_PRIVILEGE)
	public List<BahmniSyncMasterLogDTO> getManipulatedConflictsBahmniSyncMasterLog(){
		
		List<BahmniSyncMasterLog> logs = getConflictBahmniSyncMasterLog();
		
		List<BahmniSyncMasterLogDTO> newLogs = new ArrayList();
		for(BahmniSyncMasterLog log: logs){
			
			BahmniSyncMasterLogDTO newLog = new BahmniSyncMasterLogDTO();
			
			String master = log.getMasterData();
			String worker = log.getWorkerData();
						
			Map<String, String> masterMap = convertWithStream(master);
			Map<String, String> workerMap = convertWithStream(worker);
			
			String conflictingData = "";
			for (Map.Entry<String,String> entry : masterMap.entrySet()) {
				if(!entry.getValue().equals(workerMap.get(entry.getKey()))){
					String list = entry.getKey() + ": ";
					if(log.getMessage().contains("master"))
						list = list + "<b>" + entry.getValue() + "</b> , ";
					else	
						list = list + entry.getValue() + " , ";
					
					if(log.getMessage().contains("worker"))
						list = list + "<b>" + workerMap.get(entry.getKey())  + "</b> ";
					else	
						list = list + workerMap.get(entry.getKey()) ;
					 
					conflictingData = conflictingData + list + "<br/>";
				}
			}
	           
			newLog.setMasterData(conflictingData);
			
			if(log.getTable().contains("user")){
				String uuid = "";
				if(masterMap.get("user_id").equals("PK"))
					uuid = (workerMap.get("uuid"));
				else
					uuid = (workerMap.get("user_id"));
				User user = Context.getUserService().getUserByUuid(uuid);
				if(user != null)
					newLog.setStatus(user.getUsername()+"<i>("+user.getUserId()+")</i>");
				else
					newLog.setStatus("-");
			} else if(log.getTable().contains("patient")){
				
				String uuid = "";
				if(masterMap.get("patient_id").equals("PK"))
					uuid = (workerMap.get("uuid"));
				else
					uuid = (workerMap.get("patient_id"));
				Patient patient = Context.getPatientService().getPatientByUuid(masterMap.get("uuid"));
				if(patient != null){
					String patientString = patient.getGivenName() + " " + patient.getFamilyName() + "<i>("+ patient.getPatientIdentifier() +")</i>";
					newLog.setStatus(patientString);
				}
				else
					newLog.setStatus("-");
				
			} else if(log.getTable().contains("person")){
				
				String uuid = "";
				if(masterMap.get("person_id").equals("PK"))
					uuid = (workerMap.get("uuid"));
				else
					uuid = (workerMap.get("person_id"));
				
				Patient patient = Context.getPatientService().getPatientByUuid(uuid);
				System.out.println(patient);
				if(patient == null){
					
					Person person = Context.getPersonService().getPersonByUuid(uuid);
					if(person != null){
						String personString = person.getGivenName() + " " + person.getFamilyName() + "<i>(" + person.getPersonId() +")</i>";
						newLog.setStatus(personString);
					}
					else
						newLog.setStatus("-");
				}
				else {
					String patientString = patient.getGivenName() + " " + patient.getFamilyName() + "<i>("+ patient.getPatientIdentifier() +")</i>";
					newLog.setStatus(patientString);
				}
			} else if(log.getTable().equals("encounter")){
				
				Encounter enc = Context.getEncounterService().getEncounterByUuid(workerMap.get("uuid"));
				if(enc != null){
					Patient patient = enc.getPatient();
					String patientString = patient.getGivenName() + " " + patient.getFamilyName() + "<i>("+ patient.getPatientIdentifier() +")</i>";
					patientString =	patientString + "</br>" +enc.getEncounterType().getName() + "<i>(" + enc.getEncounterId() + ")</i>" ;
					newLog.setStatus(patientString);
				}
				else
					newLog.setStatus("-");

			} else if(log.getTable().equals("obs")){
				Obs obs = Context.getObsService().getObsByUuid(workerMap.get("uuid"));
				if(obs != null){
					Person person = obs.getPerson();
					Boolean flag = person.isPatient();
					String patientString = person.getGivenName() + " " + person.getFamilyName() ;
					if(flag){
						Patient patient = Context.getPatientService().getPatientByUuid(person.getUuid());
						patientString = patientString + "<i>("+ patient.getPatientIdentifier() +")</i>";
					}
					Encounter enc = obs.getEncounter();
					patientString =	patientString + "</br>" +enc.getEncounterType().getName() + "<i>(" + enc.getEncounterId() + ")</i>" ;
					Concept concept = obs.getConcept();
					patientString =	patientString + "</br>" +concept.getDisplayString()+ "<i>(" + concept.getConceptId() + ")</i>" ;
					newLog.setStatus(patientString);
				}
			} else {
				newLog.setStatus("-");
			}
			
			newLog.setLogDateTime(log.getLogDateTime());
			newLog.setWorkerId(log.getWorkerId());
			newLog.setTable(log.getTable());
			newLog.setWorkerData(log.getWorkerData());
			newLog.setMessage(log.getMessage());
			newLog.setBahmniSyncLogId(log.getBahmniSyncLogId());
			
			newLogs.add(newLog);
		}
	
		return newLogs;
	}
	
	@Authorized(BahmniSyncMasterConstants.MANAGE_BAHMNI_SYNC_PRIVILEGE)
	public List<BahmniSyncMasterLog> getErrorBahmniSyncMasterLog() throws APIException {
		Criteria criteria = sessionFactory.getCurrentSession().createCriteria(BahmniSyncMasterLog.class);
		criteria.add(Restrictions.eq("status", "ERROR"));
		criteria.addOrder(Order.desc("logDateTime"));
		return criteria.list();
	}
	
	@Authorized(BahmniSyncMasterConstants.MANAGE_BAHMNI_SYNC_PRIVILEGE)
	public List<BahmniSyncMasterLog> getAllBahmniSyncMasterLog() throws APIException {
		Criteria criteria = sessionFactory.getCurrentSession().createCriteria(BahmniSyncMasterLog.class);
		criteria.addOrder(Order.desc("logDateTime"));
		return criteria.list();
	}
	
	@Transactional
    @Authorized(BahmniSyncMasterConstants.MANAGE_BAHMNI_SYNC_PRIVILEGE)
	public void startDataPush(final Map<String, Object> obj) {
		sessionFactory.getCurrentSession().doWork(new Work() {
			
			public void execute(Connection connection) {
				try {
					System.out.println("-------------------------------------"); 
					System.out.println("Data from client"); 
					System.out.println(obj.size()); 
					System.out.println("-------------------------------------"); 
					DebeziumObject debeziumObject = getDebziumObjectFromMap(obj,connection);
					executeDebeziumObject(debeziumObject, connection);	
					
					if (sessionFactory.getCurrentSession() != null) {
						sessionFactory.getCurrentSession().clear(); // internal cache clear
					}

					if (sessionFactory.getHibernateSessionFactory().getCache() != null) {
						sessionFactory.getHibernateSessionFactory().getCache().evictQueryRegions(); 
						sessionFactory.getHibernateSessionFactory().getCache().evictEntityRegions();
					}
							
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
		List<String> pks = (List<String>)map.get("pk");

		return new DebeziumObject(String.valueOf(map.get("op")),
														String.valueOf(map.get("db")),
														String.valueOf(map.get("table")),
														mapData, mapPreviousData, fk,
														pks,
														query, String.valueOf(map.get("worker_id")));
				
	}
	
	public String replacePrimaryKeyTagsInQuery (ArrayList<Map<String, Object>> fk, String query, Connection con){
		
		for(Map<String,Object> keys : fk){
			
			String q = "SELECT " + String.valueOf(keys.get("REFERENCED_COLUMN_NAME")) + " from " +
					String.valueOf(keys.get("REFERENCED_TABLE_NAME")) + " where uuid = '" + 
					String.valueOf(keys.get("REFERENCED_UUID")) + "';";
			String qValue = getValue(q,con);
				
			System.out.println(qValue);
			System.out.println(keys.get("COLUMN_NAME"));
			System.out.println(String.valueOf(keys.get("COLUMN_NAME")));
			System.out.println(String.valueOf(keys.get("REFERENCED_UUID")));
			
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
			
			System.out.println("----------------Server Data-------------------");
			System.out.println(serverData);
			System.out.println("----------------Client Data-------------------");
			System.out.println(clientData);
			
			if(serverData.equals(clientData))
				return;
			
			if(!serverData.equals(clientPreviousData)){
				
				String conflictResolution = Context.getAdministrationService().getGlobalProperty(BahmniSyncMasterConstants.CONFLICT_RESOLUTION_RULE);
			  
				if(conflictResolution.equals("master always"))
					getLogString(dbObj, serverData, "Kept the one form master due to latest date.", "CONFLICT", con);
				else if(conflictResolution.equals("worker always")){
					ret = DatabaseUtil.runCommand(CommandType.UPDATE, dbObj.getQuery(),con);
			    	getLogString(dbObj, serverData, "Kept the one form worker due to latest date.", "CONFLICT", con);
				}
				else {
		            String dateWorker = (String)dbObj.getData().get("date_changed"); 
					String dateServer = getValue("SELECT date_changed from " + dbObj.getTable() + " where uuid = '" + (String)dbObj.getData().get("uuid")+ "'", con);
					
					Date dateUpdatedWorker = null;
					if(dateWorker != null)
						dateUpdatedWorker = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateWorker); 
					Date dateUpdatedServer = null;
					if(dateServer != null)
						dateUpdatedServer = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateServer);
					
				    Date dateVoidedWorker = null;
				    Date dateVoidedServer = null;
				    
				    if(dbObj.getData().containsKey("date_voided") && dbObj.getData().get("date_voided")!= null){
				    
			            String voidedWorker = (String)dbObj.getData().get("date_voided"); 
						String voidedServer = getValue("SELECT date_voided from " + dbObj.getTable() + " where uuid = '" + (String)dbObj.getData().get("uuid")+ "'", con);
						
						if(voidedWorker != null)
							dateVoidedWorker = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(voidedWorker); 
						if(voidedServer != null)
							dateVoidedServer = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(voidedServer);  
					
				    }
				    else if(dbObj.getData().containsKey("date_retired") && dbObj.getData().get("date_retired")!= null){
				    
			            String voidedWorker = (String)dbObj.getData().get("date_retired");
						String voidedServer = getValue("SELECT date_retired from " + dbObj.getTable() + " where uuid = '" + (String)dbObj.getData().get("uuid")+ "'", con);
						 
						if(voidedWorker != null)
							dateVoidedWorker = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(voidedWorker); 
						if(voidedServer != null)
							dateVoidedServer = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(voidedServer);  
						
				    }
				    
				    Date maxServerDate = max(dateUpdatedServer, dateVoidedServer);
				    Date maxClientDate = max(dateUpdatedWorker, dateVoidedWorker);
				    
					Object[][] dataServer = DatabaseUtil.getTableData("SELECT * from " + dbObj.getTable() + " where uuid = '" + (String)dbObj.getData().get("uuid")+"'", con);

				    
				    if(conflictResolution.equals("latest date_updated")){
					    						
					    if(maxServerDate == null){
					    	ret = DatabaseUtil.runCommand(CommandType.UPDATE, dbObj.getQuery(),con);
					    	getLogString(dbObj, serverData, "Kept the one form worker due to latest date.", "CONFLICT", con);
					    }
					    else if(maxClientDate.after(maxServerDate)){
					    	ret = DatabaseUtil.runCommand(CommandType.UPDATE, dbObj.getQuery(),con);
					    	getLogString(dbObj, serverData, "Kept the one form worker due to latest date.", "CONFLICT", con);	    
					    }else{
					    	getLogString(dbObj, serverData, "Kept the one form master due to latest date.", "CONFLICT", con);
					    }
				    } else {
					    						
					    if(maxServerDate == null){
					    	ret = DatabaseUtil.runCommand(CommandType.UPDATE, dbObj.getQuery(),con);
					    	getLogString(dbObj, serverData, "Kept the one form worker due to earliest date.", "CONFLICT", con);
					    }
					    else if(maxClientDate.before(maxServerDate)){
					    	ret = DatabaseUtil.runCommand(CommandType.UPDATE, dbObj.getQuery(),con);
					    	getLogString(dbObj, serverData, "Kept the one form worker due to earliest date.", "CONFLICT", con);	    
					    }else{
					    	getLogString(dbObj, serverData, "Kept the one form master due to earliest date.", "CONFLICT", con);
					    }
				    }
				}
			    
			}
			else{
				ret = DatabaseUtil.runCommand(CommandType.UPDATE, dbObj.getQuery(),con);
				if(ret instanceof Exception)
					getLogString(dbObj, null, ((Exception) ret).getMessage().toString(), "ERROR", con);
				else
					getLogString(dbObj, null, "", "SUCCESS", con);
			}
			
		}  else if (dbObj.getOp().equals("c")){
			
			String serverData = getServerDataAsString(dbObj,con);
			String clientData = dbObj.getData().toString();
			
			System.out.println("----------------Server Data-------------------");
			System.out.println(serverData);
			System.out.println("----------------Client Data-------------------");
			System.out.println(clientData);
			
			if(serverData.equals(clientData))
				return;
			
			Boolean flag = isObsConflict(dbObj, con);
			System.out.println("----"+flag+"----");
			
			if(flag){
				ret = DatabaseUtil.runCommand(CommandType.CREATE, dbObj.getQuery(),con);
				if(ret instanceof Exception)
					getLogString(dbObj, null, ((Exception) ret).getMessage().toString(), "ERROR", con);
				else
					getLogString(dbObj, null, "", "SUCCESS", con);
			}
			
			
		}  else if (dbObj.getOp().equals("d")){
		
			ret = DatabaseUtil.runCommand(CommandType.DELETE, dbObj.getQuery(), con);
			if(ret instanceof Exception)
				getLogString(dbObj, null, ((Exception) ret).getMessage().toString(), "ERROR", con);
			else
				getLogString(dbObj, null, "", "SUCCESS", con);
			
		}
		
	}
	
	public Boolean isObsConflict(DebeziumObject dbObj, Connection con){
		
		Boolean flag = true;
		String objString = (String)dbObj.getTable();
		if(objString.equals("obs")){
			
			System.out.println("I'm in obs....");
			
			ArrayList<Map<String,Object>> fkJsonArray = dbObj.getFk();
			
			String query = "Select value_coded, value_datetime, value_text, value_numeric, value_drug from obs where ";
			
			int conceptId = 0;
			int encounterId = 0;
			int personId = 0;
			
			for(Map<String,Object> keys : fkJsonArray){
				
				String colName = String.valueOf(keys.get("COLUMN_NAME"));
				
				if(colName.equals("person_id") || colName.equals("concept_id") || colName.equals("encounter_id") || colName.equals("obs_grouping_id")){
				
					String q = "SELECT " + String.valueOf(keys.get("REFERENCED_COLUMN_NAME")) + " from " +
							String.valueOf(keys.get("REFERENCED_TABLE_NAME")) + " where uuid = '" + 
							String.valueOf(keys.get("REFERENCED_UUID")) + "';";
					String qValue = getValue(q,con);
					
					query = query + String.valueOf(keys.get("COLUMN_NAME")) + " = " + qValue + " and ";
					
					if(colName.equals("concept_id")) conceptId = Integer.parseInt(qValue);
					else if(colName.equals("encounter_id")) encounterId = Integer.parseInt(qValue);
					else if(colName.equals("person_id")) personId = Integer.parseInt(qValue);
											
				}
										
			}
			
			query = query + " voided = 0;";
			
			Object[][] resultObjects = DatabaseUtil.getTableData(query, con);
			
			if(resultObjects.length != 0){
				
				BahmniSyncMasterObsConflicts log = new BahmniSyncMasterObsConflicts();
				
				log.setLogDateTime(new Date());
				log.setWorkerId(dbObj.getWorkerId());
				log.setConceptId(conceptId);
				log.setEncounterId(encounterId);
				log.setPatientId(personId);
				
				
				if(dbObj.getData().get("value_coded") != null){
					String conceptUuid = String.valueOf(dbObj.getData().get("value_coded"));
					String q = "SELECT concept_id from concept where uuid = '" + conceptUuid + "';";
					String qValue = getValue(q,con);
					log.setValuetype("concept");
					log.setWorkerData(qValue);
				} else if(dbObj.getData().get("value_datetime") != null){
					log.setValuetype("datetime");
					log.setWorkerData(String.valueOf(dbObj.getData().get("value_datetime")));
				} else if(dbObj.getData().get("value_text") != null){
					log.setValuetype("text");
					log.setWorkerData(String.valueOf(dbObj.getData().get("value_text")));
				} else if(dbObj.getData().get("value_numeric") != null){
					log.setValuetype("numeric");
					String valueNumeric = String.valueOf(dbObj.getData().get("value_numeric"));
					if(!valueNumeric.contains(".")) valueNumeric = valueNumeric + ".0";
					log.setWorkerData(valueNumeric);
				} else if(dbObj.getData().get("value_drug") != null){
					String drugUuid = String.valueOf(dbObj.getData().get("value_drug"));
					String q = "SELECT drug_id from drug where uuid = '" + drugUuid + "';";
					String qValue = getValue(q,con);
					log.setValuetype("drug");
					log.setWorkerData(qValue);
				}
				
				String value = "";
				for(Object[] object : resultObjects){
					if(dbObj.getData().get("value_coded") != null){
						if(object[0] != null){
							if(String.valueOf(object[0]).equals(log.getWorkerData())) {
								flag = false;
							} 
							value = value + object[0] + ",";
						}
					} else if(dbObj.getData().get("value_datetime") != null){
						if(object[1] != null){
							if(String.valueOf(object[1]).equals(log.getWorkerData())) {
								flag = false;
							} 
							value = value + object[1] + ",";
						}
					} else if(dbObj.getData().get("value_text") != null){
						if(object[2] != null){
							if(String.valueOf(object[2]).equals(log.getWorkerData())) {
								flag = false;
							} 
							value = value + object[2] + ",";
						}
					} else if(dbObj.getData().get("value_numeric") != null){
						if(object[3] != null){
							System.out.println(object[3]);
							System.out.println(log.getWorkerData());
							if(String.valueOf(object[3]).equals(log.getWorkerData())) {
								System.out.println("HERE!!");
								flag = false;
							} 
							value = value + object[3] + ",";
						}
					} else if(dbObj.getData().get("value_drug") != null){
						if(object[4] != null){
							if(String.valueOf(object[4]).equals(log.getWorkerData())) {
								flag = false;
							} 
							value = value + object[4] + ",";
						}
					}
				}
				log.setMasterData(value.substring(0,value.length()-1));
				
				if(flag)
					saveBahmniSyncMasterObsConflict(log);
				
			}
			
		}
		
		return flag;
		
	}
	
	public String getServerDataAsString(DebeziumObject dbObject, Connection con) throws ParseException{
		
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
			
				if(dbObject.getPk().contains(key) && dataServer[0][i] instanceof Integer){
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
						
						
						if(dataServer[0][i] instanceof java.sql.Timestamp ){
							
						    /*Timestamp timestamp = (Timestamp)dataServer[0][i];
						    
						    Calendar cal = Calendar.getInstance();
						    cal.setTimeInMillis(timestamp.getTime());
						    cal.add(Calendar.HOUR, -5);
						    dataServer[0][i] = new Timestamp(cal.getTime().getTime());*/
						   	dataServer[0][i] = String.valueOf(dataServer[0][i]).substring(0, String.valueOf(dataServer[0][i]).length()-2);
			    
						}
						serverData.append(key + "=" + dataServer[0][i] + ", ");
						
					}
				}
				i++;
		}		
		finalData = serverData.substring(0, serverData.length()-2) + "}";
		}

		return finalData ;
	}
	
	public void getLogString(DebeziumObject dbObject, String serverData, String message, String status, Connection con) throws ParseException{
		
		BahmniSyncMasterLog log = new BahmniSyncMasterLog();
		
		log.setLogDateTime(new Date());
		log.setWorkerId(dbObject.getWorkerId());
		log.setWorkerData(dbObject.getData().toString());
		if(dbObject.getOp().equals("u"))
			log.setMasterData(serverData);
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
	
	public static Date min(Date d1, Date d2) {
        if (d1 == null && d2 == null) return null;
        if (d1 == null) return d2;
        if (d2 == null) return d1;
        return (d1.after(d2)) ? d2 : d1;
    }
	
	public Map<String, String> convertWithStream(String mapAsString) {
		mapAsString = mapAsString.replace("{","");
		mapAsString = mapAsString.replace("}","");
		Map<String, String> map = new HashMap<>();
	    String[] splitArray = mapAsString.split(",");
	    for(String str : splitArray){
	    	String[] mapArray = str.split("=");
	    	if(mapArray.length == 2)
	    		map.put(mapArray[0].trim(),mapArray[1].trim());
	    }
	    return map;
	}

}
