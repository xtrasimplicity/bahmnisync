package org.openmrs.module.bahmnisyncworker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.URL;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.hibernate.Criteria;
import org.hibernate.criterion.Order;
import org.hibernate.jdbc.Work;
import org.json.JSONArray;
import org.json.JSONObject;
import org.openmrs.GlobalProperty;
import org.openmrs.annotation.Authorized;
import org.openmrs.api.APIException;
import org.openmrs.api.context.Context;
import org.openmrs.api.db.hibernate.DbSessionFactory;
import org.openmrs.module.bahmnisyncworker.util.BahmniSyncWorkerConstants;
import org.openmrs.module.bahmnisyncworker.util.CommandType;
import org.openmrs.module.bahmnisyncworker.util.DataType;
import org.openmrs.module.bahmnisyncworker.util.DatabaseUtil;
import org.openmrs.module.bahmnisyncworker.util.HttpConnection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.codehaus.jackson.map.ObjectMapper;

@Service
public class BahmniSyncWorkerService  {
	
	public static String TOPICS;
	public static String BOOTSTRAP_SERVERS;
	public static String MASTER_URL;
	public static String GROUP_ID;
	public static String MAX_POLL_SIZE;
	public static String DATABASE_NAME;
	public static String SCHEMA_NAME;
	
	public static Set<String> topicSet;
	
	Boolean pushFlag;
	Boolean pullFlag;
	
	@Autowired
	DbSessionFactory sessionFactory;
	
	public Boolean getSessionFactory(){
		if(sessionFactory == null)
			return false;
		else return true;			
	}
	
	public BahmniSyncWorkerLog saveBahmniSyncWorkerLog(BahmniSyncWorkerLog log) throws APIException {
		sessionFactory.getCurrentSession().saveOrUpdate(log);
		return log;
	}
	
	public List<BahmniSyncWorkerLog> getAllBahmniSyncWorkerLog() throws APIException {
		Criteria criteria = sessionFactory.getCurrentSession().createCriteria(BahmniSyncWorkerLog.class);
		criteria.addOrder(Order.desc("logDateTime"));
		return criteria.list();
	}
	
	@Authorized(BahmniSyncWorkerConstants.MANAGE_BAHMNI_SYNC_PRIVILEGE)
    @Transactional
	public Boolean checkSyncReadyStatus(){
		
		Boolean status = true;
		try{
			
			Set<String> props = new LinkedHashSet<String>();
			props.add(BahmniSyncWorkerConstants.WORKER_NODE_ID_GLOBAL_PROPERTY_NAME);
			props.add(BahmniSyncWorkerConstants.MASTER_URL_GLOBAL_PROPERTY_NAME);
			props.add(BahmniSyncWorkerConstants.KAFKA_URL_GLOBAL_PROPERTY_NAME);
			props.add(BahmniSyncWorkerConstants.DATABASE_SERVER_NAME);
			props.add(BahmniSyncWorkerConstants.CHUNK_SIZE_GLOBAL_PROPERTY_NAME);
			props.add(BahmniSyncWorkerConstants.OPENMRS_SCHEME_NAME);
			
			//remove the properties we dont want to edit
			for (GlobalProperty gp : Context.getAdministrationService().getGlobalPropertiesByPrefix(
			    BahmniSyncWorkerConstants.MODULE_ID)) {
				if (props.contains(gp.getProperty()) && gp.getPropertyValue() == null){
					status = false;
				}
				
			}
		}catch(Exception e){
			e.printStackTrace();
			status = false;			
		}
		
		if(!status){
			BahmniSyncWorkerLog logs = new BahmniSyncWorkerLog(new Date(),"Some or all required Global properties are missing.", "Failed");
			saveBahmniSyncWorkerLog(logs);	
		}
		
		return status;
		
	}
	
	@Authorized(BahmniSyncWorkerConstants.MANAGE_BAHMNI_SYNC_PRIVILEGE)
    @Transactional
	public Boolean checkKafkaConnection() throws IOException {
		
        BOOTSTRAP_SERVERS = Context.getAdministrationService().getGlobalProperty(BahmniSyncWorkerConstants.KAFKA_URL_GLOBAL_PROPERTY_NAME);

        Boolean status = true;
        
		Properties properties = new Properties();
		properties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
		properties.put("connections.max.idle.ms", 1000);
		properties.put("request.timeout.ms", 1000);
		try (AdminClient client = KafkaAdminClient.create(properties))
		{
		    ListTopicsResult topics = client.listTopics();
		    topicSet = topics.names().get();
		    if (topicSet.isEmpty())
		    {	
		    	status = false;
		    }
		}
		catch (InterruptedException | ExecutionException e)
		{	
		    status = false;
		    e.printStackTrace();
		}
		
		if(!status){
			BahmniSyncWorkerLog logs = new BahmniSyncWorkerLog(new Date(),"Couldn't connect to Kafka Server.", "Failed");
			saveBahmniSyncWorkerLog(logs);	
		}
		
		return status;
		
	}
	
	@Authorized(BahmniSyncWorkerConstants.MANAGE_BAHMNI_SYNC_PRIVILEGE)
    @Transactional
	public Boolean checkMasterConnection() throws IOException {
		
        Boolean status = false;
        
		Set<String> props = new LinkedHashSet<String>();
				
		try{
			
			String gp = Context.getAdministrationService().getGlobalProperty(BahmniSyncWorkerConstants.MASTER_URL_GLOBAL_PROPERTY_NAME);
			
			URL url = new URL(gp);
			HttpURLConnection huc = (HttpURLConnection) url.openConnection();
			int responseCode = huc.getResponseCode();
			if(responseCode == HttpURLConnection.HTTP_OK){
				status = true;
			}
			else{
				status = false;
			}
		
		}catch(Exception e){
			status = false;
			e.printStackTrace();
		}
		 
		if(!status){
			BahmniSyncWorkerLog logs = new BahmniSyncWorkerLog(new Date(),"Couldn't connect to Master Server.", "Failed");
			saveBahmniSyncWorkerLog(logs);	
		}
		
		return status;
		
	}
	
	@Authorized(BahmniSyncWorkerConstants.MANAGE_BAHMNI_SYNC_PRIVILEGE)
	@Transactional
	public Boolean startDataPush() {
		
		pushFlag = false;
		sessionFactory.getCurrentSession().doWork(new Work() {
			
			public void execute(Connection connection) {
				try {
					pushFlag = startDataPush(connection);
					
					BahmniSyncWorkerLog logs = new BahmniSyncWorkerLog(new Date(),"Data Push Success!", "Success");
					saveBahmniSyncWorkerLog(logs);
					
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					BahmniSyncWorkerLog logs = new BahmniSyncWorkerLog(new Date(),"Failed during Data Push!", "Failed");
					saveBahmniSyncWorkerLog(logs);
					pushFlag = false;
				}
			}
		});
		return pushFlag;
	}
	
	@Authorized(BahmniSyncWorkerConstants.MANAGE_BAHMNI_SYNC_PRIVILEGE)
    @Transactional
	public Boolean startDataPull() {
			
		pullFlag = true;
		sessionFactory.getCurrentSession().doWork(new Work() {
			
			public void execute(Connection connection) {
				JSONArray jsonArray = new JSONArray();
				
				try {
					String[] array = {"person",
			                "users,adress_hierarchy,address_hierarchy_level,address_hierarchy_type",
			                "relationship_type,episode,provider_attribute_type,address_hierarchy_entry,program,program_attribute_type,concept,encounter_type,encounter_role,person_name,person_address,person_attribute_type,patient,location,location_attribute_type",
							"relationship,provider,address_hierarchy_address_to_entry_map,drug,patient_progarm,person_attribute,location_attribute,concept_attribute,concept_attribute_type,concept_class,concept_complex,concept_datatype,concept_description,concept_map_type,concept_name,concept_name_tag,concept_name_tag_map,concept_numeric,concept_proposal,concept_proposal_tag_map,concept_reference_map,concept_reference_source,concept_reference_term,concept_reference_term_map,concept_set,concept_state_conversion,concept_stop_word,patient_identifier,encounter",
							"encounter_diagnosis,episode_encounter,episode_patient_program,provider_attribute,concept_answer,drug_ingredient,drug_reference_map,orders,patient_program_attribute,encounter_provider,obs",
							"drug_orders"};
							
					for(String table : array) {
					
					    do{
	
					    	String v = MASTER_URL+"/ws/rest/v1/bahmnisync/debeziumObjects/"+GROUP_ID+"/"+table+"/"+MAX_POLL_SIZE;
					    	System.out.println(v);
							String	s = HttpConnection.doGetRequest(v);
							System.out.println(s);
							jsonArray = new JSONArray(s); 
							
							System.out.println("-------------------------------------"); 
							System.out.println("Data from Master: " + table); 
							System.out.println(jsonArray.length());
							System.out.println("-------------------------------------"); 
							
							for(Object obj : jsonArray){
					    		JSONObject jb = new JSONObject(obj.toString());
					    		DebeziumObject debeziumObject = getDebziumObjectFromJSON(jb, connection);
								executeDebeziumObject(debeziumObject,connection);
							}
							
				    	}while(jsonArray.length() >= Integer.valueOf(MAX_POLL_SIZE));
					}
		    	
				} catch (Exception e) {
					// TODO Auto-generated catch block
					
					BahmniSyncWorkerLog logs = new BahmniSyncWorkerLog(new Date(),"Data Pull Failed!", "Failed");
					saveBahmniSyncWorkerLog(logs);	
					pullFlag = false;
					e.printStackTrace();
				}
		    	
				if(pullFlag){
			    	BahmniSyncWorkerLog logs = new BahmniSyncWorkerLog(new Date(),"Data Pull Success!", "Success");
					saveBahmniSyncWorkerLog(logs);
				}
				
				if (sessionFactory.getCurrentSession() != null) {
					sessionFactory.getCurrentSession().clear(); // internal cache clear
				}

				if (sessionFactory.getHibernateSessionFactory().getCache() != null) {
					sessionFactory.getHibernateSessionFactory().getCache().evictQueryRegions(); 
					sessionFactory.getHibernateSessionFactory().getCache().evictEntityRegions();
				}
				
			}
		});
		
		return pullFlag;
	}
	
	public boolean startDataPush(final Connection con) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, InterruptedException {
		boolean flag = true;
		
        try{
        	
	        BOOTSTRAP_SERVERS = Context.getAdministrationService().getGlobalProperty(BahmniSyncWorkerConstants.KAFKA_URL_GLOBAL_PROPERTY_NAME);
			MASTER_URL = Context.getAdministrationService().getGlobalProperty(BahmniSyncWorkerConstants.MASTER_URL_GLOBAL_PROPERTY_NAME);
			GROUP_ID = Context.getAdministrationService().getGlobalProperty(BahmniSyncWorkerConstants.WORKER_NODE_ID_GLOBAL_PROPERTY_NAME);
			MAX_POLL_SIZE = Context.getAdministrationService().getGlobalProperty(BahmniSyncWorkerConstants.CHUNK_SIZE_GLOBAL_PROPERTY_NAME);
			DATABASE_NAME = Context.getAdministrationService().getGlobalProperty(BahmniSyncWorkerConstants.DATABASE_SERVER_NAME);
			SCHEMA_NAME = Context.getAdministrationService().getGlobalProperty(BahmniSyncWorkerConstants.OPENMRS_SCHEME_NAME);
			
			String[] array = {"person",
	                "provider,users",
	                "provider_attribute,episode,location,location_attribute_type,location_tag,person_name,person_address,person_attribute,patient",
					"patient_program,location_attribute,location_tag_map,patient_identifier,encounter",
					"orders,episode_encounter,episode_patient_program,patient_program_attribute,encounter_provider,obs",
					"drug_order"};
			
			for(String topic : array){
							
				String topicPrefix = DATABASE_NAME + "." + SCHEMA_NAME + ".";
				topic = topicPrefix + topic;
				topic = topic.replace(",", "," + topicPrefix);
				
				ArrayList<JSONObject> postObjects = new ArrayList<JSONObject>();
	    		Consumer<String, String> consumer = createConsumer(topic);
	    		ConsumerRecords<String,String> records = null;
	    		
	    		if(consumer != null){
		    		do{  
		            	
		                //polling
		                records=consumer.poll(Duration.ofMillis(100));  
		    			System.out.println(records.count());
		
		                for(ConsumerRecord<String,String> record: records){  
		                	
		                	JSONObject postJB = new JSONObject();
		
		                	if(record.value() == null) 
		    		    		continue;
		                	                    	
		                	JSONObject json = new JSONObject(record.value());  
		    		        JSONObject payload = json.getJSONObject("payload");
		    		        String db = payload.getJSONObject("source").getString("db");
		    		        String table = payload.getJSONObject("source").getString("table");
		    		        List<String> pkColumn = null;
		    		        
		    		        try {
								pkColumn = getPrimaryKey(table, con);
							} catch (InstantiationException | IllegalAccessException | ClassNotFoundException
									| SQLException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
		    		        JSONArray fkJsonArray = new JSONArray();
		    		        JSONObject jsonDataAfter = new JSONObject();
		    		        JSONObject jsonDataBefore = new JSONObject();
		    		        
		    		        JSONObject schema = json.getJSONObject("schema");
				        
		    		        if(!payload.isNull("after"))
		    		        	jsonDataAfter = payload.getJSONObject("after");
		    		        
		    		        if(!payload.isNull("before"))
		    		        	jsonDataBefore = payload.getJSONObject("before");
		    		        
		    		        if(!payload.getString("op").equals("d")){
		    	                
		    		            String query = "SELECT " +
		    		            				"TABLE_NAME,COLUMN_NAME,CONSTRAINT_NAME, REFERENCED_TABLE_NAME,REFERENCED_COLUMN_NAME " +
		    		            				"FROM " +
		    		            				"INFORMATION_SCHEMA.KEY_COLUMN_USAGE " +
		    		            				"WHERE "+
		    		            				"REFERENCED_TABLE_SCHEMA = '" + db +"' AND " +
		    		            				"TABLE_NAME = '"+table+"';";
		    		            
		    		            Object[][] foreignkeys = getTableData(query,con);
		    		                            	                
		    		            for (Object[] fk : foreignkeys)
		    		            {
		    		            	
		    		              JSONObject fkJSONObject = new JSONObject();
		    		              
		    		              String colName = String.valueOf(fk[1]);
		    		              String referencedTableNme = String.valueOf(fk[3]);
		    		              String referencedColName = String.valueOf(fk[4]);
		    		              
		    		              String dt = getColumnDataType(json.getJSONObject("schema"),colName);
		    		              if(dt.equals(DataType.INT32.label)){
		    		            	  Object fkValue = payload.getJSONObject("after").get(colName);
		    		            	  if(!fkValue.equals(null))	{
		    		            		  
		    		            		  if(colName.equals("patient_id")){
		    		            			  referencedTableNme = "person";
		    		            			  referencedColName = "person_id";
		    		            		  }		            			  
		    		            		  
		    		            		  String fktableReference = "SELECT uuid " + 
		    			  					"from "+ db + "." + referencedTableNme + " " +
		    			  					"where " + referencedColName + " = " + fkValue;
		    		            		   
		    		            	  	String uuid = getValue(fktableReference, con);
		    		            	  	if(uuid != null){
		    			            	  	fkJSONObject.put("COLUMN_NAME", colName);
		    			                    fkJSONObject.put("REFERENCED_TABLE_NAME", referencedTableNme);
		    			                    fkJSONObject.put("REFERENCED_COLUMN_NAME", referencedColName);
		    			            	  	fkJSONObject.put("REFERENCED_UUID", uuid);
		    			            	  	jsonDataAfter.put(colName, uuid);
		    			            	  	if(payload.getString("op").equals("u"))
		    			            	  		jsonDataBefore.put(colName, uuid);
		    			            	  	fkJsonArray.put(fkJSONObject);
		    		            	  	}
		    		            	  }
		    		              }
		    		            
		    		            }
		    		            
		    		            JSONArray fieldsArray = getJSONArrayFromSchema(schema,"after");
		    		            for(int j=0; j<fieldsArray.length(); j++){
		    		        		JSONObject jObj = fieldsArray.getJSONObject(j);
		    		    			String cName = jObj.getString("field");
		    		    			
		    		    			String datatype = getColumnDataType(schema,cName);
		    		    			if(datatype.equals(DataType.INT32.label) && pkColumn.contains(cName)){
		    		    				jsonDataAfter.put(cName, "PK"); 
		    		    				if(payload.getString("op").equals("u"))
		    		    					jsonDataBefore.put(cName, "PK"); 
		    		    			}
		    		    			
		    		    			if(datatype.equals(DataType.INT32.label) && !pkColumn.contains(cName)  && !jsonDataAfter.get(cName).equals(null)){
		    		    				if(jsonDataAfter.get(cName) instanceof java.lang.Integer){
		    		    					String className = getColumnClassName(schema,cName);
		    		    					if(className != null && className.equals("org.apache.kafka.connect.data.Date")){
		        		    					Integer intDate = jsonDataAfter.getInt(cName);
		        		    					long epochMillis = TimeUnit.DAYS.toMillis(intDate);
		        		    					Date date = new Date(epochMillis);
		        		    					DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");  
		        		    		            String strDate = dateFormat.format(date);  
			        			                jsonDataAfter.put(cName, strDate);
		    		    					}
		    		    				}
		    		    			}
		    		    			if(payload.getString("op").equals("u")){
		    		    				if(datatype.equals(DataType.INT32.label) && !pkColumn.contains(cName)  && !jsonDataBefore.get(cName).equals(null)){
		    		    					if(jsonDataBefore.get(cName) instanceof java.lang.Integer){
		    		    						String className = getColumnClassName(schema,cName);
		        		    					if(className != null && className.equals("org.apache.kafka.connect.data.Date")){
		            		    					Integer intDate = jsonDataBefore.getInt(cName);
		            		    					long epochMillis = TimeUnit.DAYS.toMillis(intDate);
		            		    					Date date = new Date(epochMillis);
		            		    					DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");  
		            		    		            String strDate = dateFormat.format(date);  
		            		    		            jsonDataBefore.put(cName, strDate);
		        		    					}
		        		    				}
		        		    			}
		    		    			}
		    		    			if(datatype.equals(DataType.INT64.label) && !jsonDataAfter.get(cName).equals(null)){
		    		    				String className = getColumnClassName(schema,cName);
				    					if(className != null && className.equals("org.apache.kafka.connect.data.Timestamp")){
		        			                String strDate = convertTimeWithTimeZome(jsonDataAfter.getLong(cName));
		        			                jsonDataAfter.put(cName, strDate);
				    					}
		    						 } 
		    		    			
		    		    			if(payload.getString("op").equals("u")){
		        		    			if(datatype.equals(DataType.INT64.label) && !jsonDataBefore.get(cName).equals(null)){
		        		    				String className = getColumnClassName(schema,cName);
		        		    				if(className != null && className.equals("org.apache.kafka.connect.data.Timestamp")){
			        			                String strDate = convertTimeWithTimeZome(jsonDataBefore.getLong(cName));
			        			                jsonDataBefore.put(cName, strDate);
		        		    				}
		        						 }
		    		    			}
		    		    			
		    		    			if(datatype.equals(DataType.INT16.label) && !jsonDataAfter.get(cName).equals(null)){
		    		    				if (jsonDataAfter.getInt(cName) == 0)
		    		    					jsonDataAfter.put(cName, "false");
		    						 	else
		    						 		jsonDataAfter.put(cName, "true");
		    						 }
		    		    			
		    		    			if(payload.getString("op").equals("u")){
		        		    			if(datatype.equals(DataType.INT16.label) && !jsonDataBefore.get(cName).equals(null)){
		        		    				if (jsonDataBefore.getInt(cName) == 0)
		        		    					jsonDataBefore.put(cName, "false");
		        						 	else
		        						 		jsonDataBefore.put(cName, "true");
		        						 }
		    		    			}
		    					}
		    		        
		    		        } 
		    		        
		    		        String queryForPost = getQueryForPost(payload.getString("op"), payload.getJSONObject("source").getString("table"), schema, jsonDataAfter, 
		    		        		jsonDataBefore, pkColumn, fkJsonArray, con);
		    					   
		    		        postJB.put("fk", fkJsonArray);
		    		        postJB.put("data", jsonDataAfter);
		    		        postJB.put("op", payload.getString("op"));
		    		        postJB.put("db", payload.getJSONObject("source").getString("db"));
		    		        postJB.put("table", payload.getJSONObject("source").getString("table"));
		    		        postJB.put("pk", pkColumn);
		    		        postJB.put("query", queryForPost);
		    		        postJB.put("previous_data", jsonDataBefore);
		    		        postJB.put("worker_id", GROUP_ID);
		    		        
		    		        postObjects.add(postJB); 
		    		        
		    		        
		                }  
		              
		              if(!postObjects.isEmpty()){
		            	  System.out.println("-------------------------------------"); 
		                  System.out.println("client Data Push"); 
		                  System.out.println(postObjects);
		                  System.out.println("-------------------------------------"); 
		            	  HttpConnection.doPost(MASTER_URL+"/ws/rest/v1/bahmnisync/debeziumObjects",postObjects.toString());
		              }
		              
		       		  postObjects.clear();
		                 
		            }  while (!records.isEmpty());
		    		consumer.commitSync();
		            consumer.close();
	    		}
			}
        }catch(Exception e){
        	
        	flag = false;
			e.printStackTrace();
			
        }
		
		return flag;
	}
	
	private static Consumer<String, String> createConsumer(String topic) {
		final Properties props = new Properties();
		
		GROUP_ID = Context.getAdministrationService().getGlobalProperty(BahmniSyncWorkerConstants.WORKER_NODE_ID_GLOBAL_PROPERTY_NAME);

		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID + "worker-side-001");
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_SIZE);
		props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

		
		// Create the consumer using props.
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		
		
		String[] array = topic.split(",");
		List<String> arrayList = new ArrayList<String>();
		
		for(String arr: array){
			if(topicSet.contains(arr))
				arrayList.add(arr);
		}
		
		System.out.println("---"+arrayList+"----");
		
		// Subscribe to the topic
		if(!arrayList.isEmpty())
			consumer.subscribe(arrayList);
		else
			consumer = null;
				
		return consumer;
	}
	
	public static List<String> getPrimaryKey(String tableName, Connection con) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException{
    	ResultSet rs = null;
    	String pk = null;
    	DatabaseMetaData meta = con.getMetaData();
    	List<String> pkList = new ArrayList();
    	rs = meta.getPrimaryKeys(null, null, tableName);
    	while (rs.next()) {
    	      pk = rs.getString("COLUMN_NAME");
    	      if(!pkList.contains(pk))
    	    	  pkList.add(pk);
    	    }
    	return pkList;
    }
	
	private static String getColumnDataType(JSONObject schema, String columnName) {
		
		JSONArray fieldsArray = getJSONArrayFromSchema(schema, "after");
		if (fieldsArray == null)
			return null;
		
		for (int j = 0; j < fieldsArray.length(); j++) {
			JSONObject jObj = fieldsArray.getJSONObject(j);
			String colName = jObj.getString("field");
			if (colName.equals(columnName)) {
				return jObj.getString("type");
			}
		}
		
		return null;
	}
	
	private static String getColumnClassName(JSONObject schema, String columnName) {
		
		JSONArray fieldsArray = getJSONArrayFromSchema(schema, "after");
		if (fieldsArray == null)
			return null;
		
		for (int j = 0; j < fieldsArray.length(); j++) {
			JSONObject jObj = fieldsArray.getJSONObject(j);
			String colName = jObj.getString("field");
			if (colName.equals(columnName)) {
				if(jObj.has("name"))
					return jObj.getString("name");
			}
		}
		
		return null;
	}
	
	private static JSONArray getJSONArrayFromSchema(JSONObject schema, String fieldName) {
		
		JSONArray jsonArray = schema.getJSONArray("fields");
		for (int i = 0; i < jsonArray.length(); i++) {
			JSONObject jObject = jsonArray.getJSONObject(i);
			String field = jObject.getString("field");
			if (field.equals(fieldName))
				return jObject.getJSONArray("fields");
		}
		
		return null;
		
	}
	
	public static Object[][] getTableData(String command, Connection con) {
        // 2 Dimensional Object array to hold the table data
        Object[][] data;
        // Array list of array lists to record data during transaction
        ArrayList<ArrayList<Object>> array = new ArrayList<ArrayList<Object>>();
        try {
            Statement st = con.createStatement();
            ResultSet rs = st.executeQuery(command);
            // Get the number of columns
            int columns = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                // Array list to temporarily hold a record
                ArrayList<Object> record = new ArrayList<Object>();
                for (int i = 0; i < columns; i++)
                    record.add(rs.getObject(i + 1));
                // Add the record to main Array list
                array.add(record);
            }
            // Copy main Array list to an Object array
            Object[] list = array.toArray();
            // Define how many records will be there in table
            data = new Object[list.length][];
            for (int i = 0; i < list.length; i++) {
                // Cast each element in an Array list
                ArrayList<Object> fieldList = (ArrayList<Object>) list[i];
                // Copy record into table
                data[i] = fieldList.toArray();
            }
            rs.close();
        } catch (Exception e) {
            data = null;
        } 
        return data;
    }
	
	public static String getValue(String command, Connection con) {
        String str = null;
        try {
            Statement st = con.createStatement();
            ResultSet rs = st.executeQuery(command);
            rs.next();
            str = rs.getString(1);
            rs.close();
        } catch (Exception e) {
        	 str = null;
        } 
        return str;
    }
	
	private static String convertTimeWithTimeZome(long time) {
		
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		Instant instant = Instant.ofEpochMilli(time);
        Date dateObject = Date.from(instant);
        format.setTimeZone(TimeZone.getTimeZone("UTC"));
		
		return format.format(dateObject);		
	}
	
	private static String getQueryForPost(String op, String table, JSONObject schema, JSONObject data,
	        JSONObject dataBefore, List<String> pkColumn, JSONArray fkJsonArray, Connection con) throws SQLException {
		
		if (op.equals("u")) {
			
			return makeUpdateQuery(table, schema, data, pkColumn, fkJsonArray, con);
			
		} else if (op.equals("c")) {
			
			return makeCreateQuery(table, schema, data, pkColumn, fkJsonArray);
			
		} else if (op.equals("d")) {
			
			return makeDeleteQuery(table, schema, pkColumn, dataBefore, con);
		}
		
		return "";
		
	}
	
	private static String makeDeleteQuery(String table, JSONObject schema, List<String> pkColumn, JSONObject data, Connection con) throws SQLException {
		
		StringBuilder deleteQuery = new StringBuilder("DELETE FROM " + table + " WHERE ");
		
		String query = "SHOW COLUMNS FROM " + table + " LIKE 'uuid'";
		
		Statement st = con.createStatement();
        ResultSet rs = st.executeQuery(query);
       
        if (rs.next() == false) {
           
        	for(String pk : pkColumn)
        		deleteQuery.append(pk + " = <" + pk + "> and ");

        	deleteQuery.append("1 = 1");
        	
        }else {
        	deleteQuery.append("uuid = '" + data.getString("uuid") + "';");
        }
		
		return deleteQuery.toString();
		
	}
	
	private static String makeCreateQuery(String table, JSONObject schema, JSONObject data, List<String> pkColumn,
	        JSONArray fkJsonArray) {
		
		StringBuilder fieldsString = new StringBuilder();
		StringBuilder valuesString = new StringBuilder();
		
		fieldsString.append("INSERT INTO " + table + "(");
		valuesString.append("VALUES (");
		
		JSONArray fieldsArray = getJSONArrayFromSchema(schema, "after");
		
		for (int j = 0; j < fieldsArray.length(); j++) {
			JSONObject jObj = fieldsArray.getJSONObject(j);
			String cName = jObj.getString("field");
			
			if(pkColumn.contains(cName)){
				
				if (isForeignKey(cName, fkJsonArray)) {
					fieldsString.append(cName + ",");
					valuesString.append("<" + cName + ">" + ",");
					continue;
				}
				
			}
			
			if ((!pkColumn.contains(cName)) && (!data.get(cName).equals(null))) {
				fieldsString.append(cName + ",");
				
				if (isForeignKey(cName, fkJsonArray)) {
					valuesString.append("<" + cName + ">" + ",");
					continue;
				}
				
				String datatype = getColumnDataType(schema, cName);
				if (datatype.equals(DataType.INT32.label)){
					if (data.get(cName) instanceof Integer){
						
						if(cName.equals("location_id") && data.getInt(cName) == 0)
							valuesString.append("NULL,");
						else
							valuesString.append(data.getInt(cName) + ",");
						
					}
					else
						valuesString.append("'" + data.getString(cName) + "',");
				}
				else if (datatype.equals(DataType.DOUBLE.label))
					valuesString.append(data.get(cName) + ",");
				else if (datatype.equals(DataType.STRING.label))
					valuesString.append("'" + data.getString(cName) + "',");
				else if (datatype.equals(DataType.INT64.label)) {
					String strDate = "";
					if (data.get(cName) instanceof Long)
						strDate = String.valueOf(data.getLong(cName));
					else
						strDate = data.getString(cName);
					valuesString.append("'" + strDate + "',");
				} else if (datatype.equals(DataType.INT16.label)) {
					if (data.get(cName) instanceof Integer) {
						if (data.getInt(cName) == 0)
							valuesString.append("FALSE" + ",");
						else
							valuesString.append("TRUE" + ",");
					} else
						valuesString.append(data.getString(cName).toUpperCase() + ",");
				}
			}
		}
		fieldsString = new StringBuilder(fieldsString.substring(0, fieldsString.length() - 1));
		valuesString = new StringBuilder(valuesString.substring(0, valuesString.length() - 1));
		
		return fieldsString.toString() + ") " + valuesString.toString() + ");";
		
	}
	
	private static String makeUpdateQuery(String table, JSONObject schema, JSONObject data, List<String> pkColumn,
	        JSONArray fkJsonArray, Connection con) throws SQLException {
		
		JSONArray fieldsArray = getJSONArrayFromSchema(schema, "after");
		StringBuilder updateQuery = new StringBuilder("update " + table + " set ");
		
		if (fieldsArray == null)
			return null;
		
		for (int j = 0; j < fieldsArray.length(); j++) {
			JSONObject jObj = fieldsArray.getJSONObject(j);
			String cName = jObj.getString("field");
						
			if ((!pkColumn.contains(cName)) && (!data.get(cName).equals(null))) {
				updateQuery.append(cName + " = ");
				
				if (isForeignKey(cName, fkJsonArray)) {
					updateQuery.append("<" + cName + ">" + ",");
					continue;
				}
				
				String datatype = getColumnDataType(schema, cName);
				if (datatype.equals(DataType.INT32.label)){
					if (data.get(cName) instanceof Integer){
												
						if(cName.equals("location_id") && data.getInt(cName) == 0)
							updateQuery.append("NULL,");
						else
							updateQuery.append(data.getInt(cName) + ",");
						
					}
					else
						updateQuery.append("'" + data.getString(cName) + "',");
				}
				else if (datatype.equals(DataType.DOUBLE.label)){
					updateQuery.append(data.get(cName) + ",");
				}
				else if (datatype.equals(DataType.STRING.label))
					updateQuery.append("'" + data.getString(cName) + "',");
				else if (datatype.equals(DataType.INT64.label)) {
					String strDate = "";
					if (data.get(cName) instanceof Long)
						strDate = convertTimeWithTimeZome(data.getLong(cName));
					else
						strDate = data.getString(cName);
					updateQuery.append("'" + strDate + "',");
				} else if (datatype.equals(DataType.INT16.label)) {
					if (data.get(cName) instanceof Integer) {
						if (data.getInt(cName) == 0)
							updateQuery.append("FALSE" + ",");
						else
							updateQuery.append("TRUE" + ",");
					} else
						updateQuery.append(data.getString(cName).toUpperCase() + ",");
				}
			}
		}
		
		updateQuery = new StringBuilder(updateQuery.substring(0, updateQuery.length() - 1));
		
		if(table.equals("patient")){
			updateQuery.append(" where patient_id = <person_id>");
		}
		else{
			
			updateQuery.append( "where ");
			
			String query = "SHOW COLUMNS FROM " + table + " LIKE 'uuid'";

			Statement st = con.createStatement();
	        ResultSet rs = st.executeQuery(query);
	       
	        if (rs.next() == false) {
	           
	        	for(String pk : pkColumn)
	        		updateQuery.append(pk + " = <" + pk + "> and ");

	        	updateQuery.append("1 = 1");
	        	
	        }else {
	        	updateQuery.append("uuid = '" + data.getString("uuid") + "';");
	        }

		}
		
		return updateQuery.toString();
	}
	
	private static Boolean isForeignKey(String colName, JSONArray fkJsonArray) {
		
		for (int i = 0; i < fkJsonArray.length(); i++) {
			JSONObject jObject = fkJsonArray.getJSONObject(i);
			if (jObject.getString("COLUMN_NAME").equals(colName))
				return true;
		}
		
		return false;
	}
	
	public DebeziumObject getDebziumObjectFromJSON(JSONObject map, Connection con){
		
		ObjectMapper oMapper = new ObjectMapper();
		Map<String, Object> mapData = toMap(map.getJSONObject("data"));
		JSONArray fkArray = map.getJSONArray("fk");
		ArrayList<Map<String,Object>> fk = new ArrayList<Map<String,Object>>();     
		if (fkArray != null) { 
		   for (int i=0;i<fkArray.length();i++){ 
			  fk.add(toMap(fkArray.getJSONObject(i)));
		   } 
		} 
		String query = replacePrimaryKeyTagsInQuery(fk, String.valueOf(map.get("query")), con);
		JSONArray pkArray = map.getJSONArray("pk");
		List<String> pks = new ArrayList();
		if (pkArray != null) { 
		   for (int i=0;i<pkArray.length();i++){ 
			  pks.add(pkArray.get(i).toString());
		   } 
		} 
						
		DebeziumObject debeziumObject = new DebeziumObject(map.getString("op"),
														map.getString("db"),
														map.getString("table"),
														mapData,fk,
														pks,
														query);
				
		return debeziumObject;
		
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
	
	public  void executeDebeziumObject(DebeziumObject dbObj, Connection con){
		
		Object ret = null;
						
		if(dbObj.getOp().equals("u")){
			
			String serverData = getServerDataAsString(dbObj,con);
			String clientData = dbObj.getData().toString();
			
			System.out.println("----------------Server Data-------------------");
			System.out.println(serverData);
			System.out.println("----------------Client Data-------------------");
			System.out.println(clientData);
			
			if(serverData.equals(clientData))
				return;
			
			ret = DatabaseUtil.runCommand(CommandType.UPDATE, dbObj.getQuery(), con);
			
		}  else if (dbObj.getOp().equals("c")){
			
			String serverData = getServerDataAsString(dbObj,con);
			String clientData = dbObj.getData().toString();
			
			System.out.println("----------------Server Data-------------------");
			System.out.println(serverData);
			System.out.println("----------------Client Data-------------------");
			System.out.println(clientData);
			
			if(serverData.equals(clientData))
				return;
			
			ret = DatabaseUtil.runCommand(CommandType.CREATE, dbObj.getQuery(), con);
			
		}  else if (dbObj.getOp().equals("d")){
		
			ret = DatabaseUtil.runCommand(CommandType.DELETE, dbObj.getQuery(), con);
			
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
					    dataServer[0][i] = new Timestamp(cal.getTime().getTime());	*/		    
					    
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

	public static Boolean isForeignKey(String colName, ArrayList<Map<String,Object>> fkJsonArray){
		
		for(int i=0; i<fkJsonArray.size(); i++){
			Map<String,Object> jObject = fkJsonArray.get(i);
			if(jObject.get("COLUMN_NAME").equals(colName))
				return true;
		}
		
		return false;
	}

}
