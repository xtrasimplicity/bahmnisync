package org.openmrs.module.bahmnisyncworker;

import java.io.IOException;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.hibernate.Criteria;
import org.hibernate.criterion.Order;
import org.hibernate.jdbc.Work;
import org.json.JSONArray;
import org.json.JSONObject;
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
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
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
	
	@Autowired
	DbSessionFactory sessionFactory;
	
	
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
	public void startDataPush() {
		sessionFactory.getCurrentSession().doWork(new Work() {
			
			public void execute(Connection connection) {
				try {
					startDataPush(connection);
					
					BahmniSyncWorkerLog logs = new BahmniSyncWorkerLog(new Date(),"Data Push Success!");
					saveBahmniSyncWorkerLog(logs);
					
				} catch (SQLException | InstantiationException | IllegalAccessException | ClassNotFoundException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					BahmniSyncWorkerLog logs = new BahmniSyncWorkerLog(new Date(),"Failed during Data Push!");
					saveBahmniSyncWorkerLog(logs);
				}
			}
		});
	}
	
	@Authorized(BahmniSyncWorkerConstants.MANAGE_BAHMNI_SYNC_PRIVILEGE)
    @Transactional
	public void startDataPull() {
			
		sessionFactory.getCurrentSession().doWork(new Work() {
			
			public void execute(Connection connection) {
				JSONArray jsonArray = new JSONArray();
				
				try {
					String	tables = HttpConnection.doGetRequest(MASTER_URL+"/ws/rest/v1/bahmnisync/tablestopull");

					System.out.println(tables);
					String[] array = tables.split(",");
					System.out.println(array);
					
					for(String table : array) {
					
					    do{
	
							String	s = HttpConnection.doGetRequest(MASTER_URL+"/ws/rest/v1/bahmnisync/debeziumObjects/"+GROUP_ID+"/"+table+"/"+MAX_POLL_SIZE);
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
		    	
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    	
		    	BahmniSyncWorkerLog logs = new BahmniSyncWorkerLog(new Date(),"Data Pull Success!");
				saveBahmniSyncWorkerLog(logs);	
				
				if (sessionFactory.getCurrentSession() != null) {
					sessionFactory.getCurrentSession().clear(); // internal cache clear
				}

				if (sessionFactory.getHibernateSessionFactory().getCache() != null) {
					sessionFactory.getHibernateSessionFactory().getCache().evictQueryRegions(); 
					sessionFactory.getHibernateSessionFactory().getCache().evictEntityRegions();
				}
				
				return;
			}
		});
	}
	
	public boolean startDataPush(final Connection con) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, InterruptedException {
		boolean flag = true;
		
        
        BOOTSTRAP_SERVERS = Context.getAdministrationService().getGlobalProperty(BahmniSyncWorkerConstants.KAFKA_URL_GLOBAL_PROPERTY_NAME);
		TOPICS =  Context.getAdministrationService().getGlobalProperty(BahmniSyncWorkerConstants.SYNC_TABLE_GLOBAL_PROPERTY_NAME);
		MASTER_URL = Context.getAdministrationService().getGlobalProperty(BahmniSyncWorkerConstants.MASTER_URL_GLOBAL_PROPERTY_NAME);
		GROUP_ID = Context.getAdministrationService().getGlobalProperty(BahmniSyncWorkerConstants.WORKER_NODE_ID_GLOBAL_PROPERTY_NAME);
		MAX_POLL_SIZE = Context.getAdministrationService().getGlobalProperty(BahmniSyncWorkerConstants.CHUNK_SIZE_GLOBAL_PROPERTY_NAME);
		
		String[] array = {"dbserver1.openmrs.person",
                "dbserver1.openmrs.users,dbserver1.openmrs.person_name,dbserver1.openmrs.person_address,dbserver1.openmrs.person_attribute,dbserver1.openmrs.patient",
				"dbserver1.openmrs.patient_identifier,dbserver1.openmrs.encounter",
				"dbserver1.openmrs.obs"};
		
		//String[] array = TOPICS.split(",");
		
		for(String topic : array){
			
			ArrayList<JSONObject> postObjects = new ArrayList<JSONObject>();
    		Consumer<String, String> consumer = createConsumer(topic);
    		ConsumerRecords<String,String> records = null;
    		
    		do{  
            	
                //polling
                records=consumer.poll(Duration.ofMillis(100));  
                for(ConsumerRecord<String,String> record: records){  
                	
                	JSONObject postJB = new JSONObject();
                	
                	if(record.value() == null) 
    		    		continue;
                	                    	
                	JSONObject json = new JSONObject(record.value());  
    		        JSONObject payload = json.getJSONObject("payload");
    		        String db = payload.getJSONObject("source").getString("db");
    		        String table = payload.getJSONObject("source").getString("table");
    		        String pkColumn = null;
    		        
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
    		    			if(datatype.equals(DataType.INT32.label) && cName.equals(pkColumn)){
    		    				jsonDataAfter.put(cName, "PK"); 
    		    				if(payload.getString("op").equals("u"))
    		    					jsonDataBefore.put(cName, "PK"); 
    		    			}
    		    			
    		    			if(datatype.equals(DataType.INT32.label) && !cName.equals(pkColumn)  && !jsonDataAfter.get(cName).equals(null)){
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
    		    				if(datatype.equals(DataType.INT32.label) && !cName.equals(pkColumn)  && !jsonDataBefore.get(cName).equals(null)){
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
    		        
    		        System.out.println("Before...");
    		        String queryForPost = getQueryForPost(payload.getString("op"), payload.getJSONObject("source").getString("table"), schema, jsonDataAfter, 
    		        		jsonDataBefore, pkColumn, fkJsonArray);
    		        System.out.println("after...");
    					   
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
            consumer.close();
		}
		
		return flag;
	}
	
	private static Consumer<String, String> createConsumer(String topic) {
		final Properties props = new Properties();
		
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test567");
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_SIZE);
		
		// Create the consumer using props.
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		
		
		String[] array = topic.split(",");
		// Subscribe to the topic
		consumer.subscribe(Arrays.asList(array));
				
		return consumer;
	}
	
	public static String getPrimaryKey(String tableName, Connection con) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException{
    	ResultSet rs = null;
    	String pk = null;
    	DatabaseMetaData meta = con.getMetaData();
    	rs = meta.getPrimaryKeys(null, null, tableName);
    	while (rs.next()) {
    	      pk = rs.getString("COLUMN_NAME");
    	      break;
    	    }
    	return pk;
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
		Calendar cal = Calendar.getInstance();
		cal.setTimeZone(TimeZone.getTimeZone("UTC"));
		cal.setTimeInMillis(time);
		StringBuilder stringDate = new StringBuilder(cal.get(Calendar.YEAR) + "-");
		
		if (cal.get(Calendar.MONTH) < 9)
			stringDate.append("0" + (cal.get(Calendar.MONTH) + 1) + "-");
		else
			stringDate.append((cal.get(Calendar.MONTH) + 1) + "-");
		if (cal.get(Calendar.DAY_OF_MONTH) < 10)
			stringDate.append("0" + cal.get(Calendar.DAY_OF_MONTH) + " ");
		else
			stringDate.append(cal.get(Calendar.DAY_OF_MONTH) + " ");
		if (cal.get(Calendar.HOUR_OF_DAY) < 10)
			stringDate.append("0" + cal.get(Calendar.HOUR_OF_DAY) + ":");
		else
			stringDate.append(cal.get(Calendar.HOUR_OF_DAY) + ":");
		if (cal.get(Calendar.MINUTE) < 10)
			stringDate.append("0" + cal.get(Calendar.MINUTE) + ":");
		else
			stringDate.append(cal.get(Calendar.MINUTE) + ":");
		if (cal.get(Calendar.SECOND) < 10)
			stringDate.append("0" + cal.get(Calendar.SECOND));
		else
			stringDate.append(cal.get(Calendar.SECOND));
		
		return stringDate.toString();
		
	}
	
	private static String getQueryForPost(String op, String table, JSONObject schema, JSONObject data,
	        JSONObject dataBefore, String pkColumn, JSONArray fkJsonArray) {
		
		if (op.equals("u")) {
			
			return makeUpdateQuery(table, schema, data, pkColumn, fkJsonArray);
			
		} else if (op.equals("c")) {
			
			return makeCreateQuery(table, schema, data, pkColumn, fkJsonArray);
			
		} else if (op.equals("d")) {
			
			return makeDeleteQuery(table, schema, pkColumn, dataBefore);
		}
		
		return "";
		
	}
	
	private static String makeDeleteQuery(String table, JSONObject schema, String pkColumn, JSONObject data) {
		
		StringBuilder deleteQuery = new StringBuilder("DELETE FROM " + table + " WHERE ");
		
		String datatype = getColumnDataType(schema, pkColumn);
		if (datatype.equals(DataType.INT32.label))
			deleteQuery.append("uuid = '" + data.getString("uuid") + "';");
		else if (datatype.equals(DataType.STRING.label))
			deleteQuery.append(pkColumn + " = '" + data.getString(pkColumn) + "';");
		
		return deleteQuery.toString();
		
	}
	
	private static String makeCreateQuery(String table, JSONObject schema, JSONObject data, String pkColumn,
	        JSONArray fkJsonArray) {
		
		StringBuilder fieldsString = new StringBuilder();
		StringBuilder valuesString = new StringBuilder();
		
		fieldsString.append("INSERT INTO " + table + "(");
		valuesString.append("VALUES (");
		
		JSONArray fieldsArray = getJSONArrayFromSchema(schema, "after");
		
		for (int j = 0; j < fieldsArray.length(); j++) {
			JSONObject jObj = fieldsArray.getJSONObject(j);
			String cName = jObj.getString("field");
			
			if(pkColumn.equalsIgnoreCase(cName)){
				
				if (isForeignKey(cName, fkJsonArray)) {
					fieldsString.append(cName + ",");
					valuesString.append("<" + cName + ">" + ",");
					continue;
				}
				
			}
			
			if ((!pkColumn.equalsIgnoreCase(cName)) && (!data.get(cName).equals(null))) {
				fieldsString.append(cName + ",");
				
				if (isForeignKey(cName, fkJsonArray)) {
					valuesString.append("<" + cName + ">" + ",");
					continue;
				}
				
				String datatype = getColumnDataType(schema, cName);
				if (datatype.equals(DataType.INT32.label))
					if (data.get(cName) instanceof Integer)
						valuesString.append(data.getInt(cName) + "',");
					else
						valuesString.append("'" + data.getString(cName) + "',");
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
	
	private static String makeUpdateQuery(String table, JSONObject schema, JSONObject data, String pkColumn,
	        JSONArray fkJsonArray) {
		
		JSONArray fieldsArray = getJSONArrayFromSchema(schema, "after");
		StringBuilder updateQuery = new StringBuilder("update " + table + " set ");
		
		if (fieldsArray == null)
			return null;
		
		for (int j = 0; j < fieldsArray.length(); j++) {
			JSONObject jObj = fieldsArray.getJSONObject(j);
			String cName = jObj.getString("field");
						
			if ((!pkColumn.equalsIgnoreCase(cName)) && (!data.get(cName).equals(null))) {
				updateQuery.append(cName + " = ");
				
				if (isForeignKey(cName, fkJsonArray)) {
					updateQuery.append("<" + cName + ">" + ",");
					continue;
				}
				
				String datatype = getColumnDataType(schema, cName);
				if (datatype.equals(DataType.INT32.label))
					if (data.get(cName) instanceof Integer)
						updateQuery.append(data.getInt(cName) + "',");
					else
						updateQuery.append("'" + data.getString(cName) + "',");
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
		String datatype = getColumnDataType(schema, pkColumn);
		
		if(table.equals("patient")){
			updateQuery.append(" where patient_id = <person_id>");
		}
		else{
			if (datatype.equals(DataType.INT32.label))
				updateQuery.append(" where uuid = '" + data.getString("uuid") + "';");
			else if (datatype.equals(DataType.STRING.label))
				updateQuery.append(" where" + pkColumn + " = '" + data.getString(pkColumn) + "';");
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

						
		DebeziumObject debeziumObject = new DebeziumObject(map.getString("op"),
														map.getString("db"),
														map.getString("table"),
														mapData,fk,
														map.getString("pk"),
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
			
			if(serverData.equals(clientData))
				return;
			
			ret = DatabaseUtil.runCommand(CommandType.UPDATE, dbObj.getQuery(), con);
			
		}  else if (dbObj.getOp().equals("c")){
			
			String serverData = getServerDataAsString(dbObj,con);
			String clientData = dbObj.getData().toString();
			
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
						

						if(dataServer[0][i] instanceof java.sql.Timestamp ){
							
						    Timestamp timestamp = (Timestamp)dataServer[0][i];
						    Calendar cal = Calendar.getInstance();
						    cal.setTimeInMillis(timestamp.getTime());
						    cal.add(Calendar.HOUR, -5);
						    dataServer[0][i] = new Timestamp(cal.getTime().getTime());
						    
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
