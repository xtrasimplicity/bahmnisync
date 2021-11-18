package org.openmrs.module.bahmnisyncmaster.service;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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
import org.hibernate.Criteria;
import org.hibernate.criterion.Criterion;
import org.hibernate.criterion.LogicalExpression;
import org.hibernate.criterion.Restrictions;
import org.hibernate.jdbc.Work;
import org.json.JSONArray;
import org.json.JSONObject;
import org.openmrs.annotation.Authorized;
import org.openmrs.api.APIException;
import org.openmrs.api.context.Context;
import org.openmrs.api.db.hibernate.DbSessionFactory;
import org.openmrs.module.bahmnisyncmaster.util.BahmniSyncMasterConstants;
import org.openmrs.module.bahmnisyncmaster.util.CommandType;
import org.openmrs.module.bahmnisyncmaster.util.DataType;
import org.openmrs.module.bahmnisyncmaster.util.DatabaseUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class DataPullService  {

	@Autowired
	DbSessionFactory sessionFactory;
	
	ArrayList<JSONObject> postObjects = new ArrayList<JSONObject>();
	
	@Transactional
    @Authorized(BahmniSyncMasterConstants.MANAGE_BAHMNI_SYNC_PRIVILEGE)
	public ArrayList<JSONObject> startDataPull(final String serverid, final int chunckSize, final String table) {

		sessionFactory.getCurrentSession().doWork(new Work() {
			
			public void execute(Connection con) throws SQLException {
				
					postObjects.clear();		
					
		            final Consumer<String, String> consumer = createConsumer( serverid,  chunckSize, table);
		               
		            if(consumer != null){
			            int i = 0; 
			             do{   
			            	//int i = 0; 
			            	ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
			            	
			            	if(records.isEmpty())
			            		i++;
			            	
			            	if(i > 2)
			            		break;
			            	 
			                for(ConsumerRecord<String,String> record: records){   
			                		                	
			                	JSONObject postJB = new JSONObject();
			                	
			                	System.out.println("COUNT: " + records.count());
			                	
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
								
								if(payload.getString("op").equals("r"))
									continue;
								
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
			                        
			                        Object[][] foreignkeys = DatabaseUtil.getTableData(query,con);
			                                        	                
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
			                			if(datatype.equals(DataType.INT32.label) && pkColumn.contains(cName))
			                				jsonDataAfter.put(cName, "PK");  
			                			
			                			if(datatype.equals(DataType.INT32.label) && !pkColumn.contains(cName) && !jsonDataAfter.get(cName).equals(null)){
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
			                			
			                			if(datatype.equals(DataType.INT64.label) && !jsonDataAfter.get(cName).equals(null)){
			    		    				String className = getColumnClassName(schema,cName);
					    					if(className != null && className.equals("org.apache.kafka.connect.data.Timestamp")){
			        			                String strDate = convertTimeWithTimeZome(jsonDataAfter.getLong(cName));
			        			                jsonDataAfter.put(cName, strDate);
					    					}
			    						 } 
			                			
			                			if(datatype.equals(DataType.INT16.label) && !jsonDataAfter.get(cName).equals(null)){
			                				if (jsonDataAfter.getInt(cName) == 0)
			                					jsonDataAfter.put(cName, "false");
			            				 	else
			            				 		jsonDataAfter.put(cName, "true");
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
			                    
			                    
			                    postObjects.add(postJB);
			            
			            	}
			             }while(postObjects.size() < chunckSize);
			            	
			             consumer.commitAsync();
			             consumer.close(); 
			             
		            }
			}
		});
		System.out.println("-------------------------------------"); 
		System.out.println("Data from master"); 
		System.out.println(postObjects); 
		System.out.println("-------------------------------------");
		return postObjects;
	}
	
	public Set<String> getTopicSet() {
		
        String BOOTSTRAP_SERVERS = Context.getAdministrationService().getGlobalProperty(BahmniSyncMasterConstants.KAFKA_URL_GLOBAL_PROPERTY_NAME);

        Set<String> topicSet = null;
        
		Properties properties = new Properties();
		properties.put("bootstrap.servers", BOOTSTRAP_SERVERS);
		properties.put("connections.max.idle.ms", 1000);
		properties.put("request.timeout.ms", 1000);
		try (AdminClient client = KafkaAdminClient.create(properties))
		{
		    ListTopicsResult topics = client.listTopics();
		    topicSet = topics.names().get();
		}
		catch (InterruptedException | ExecutionException e)
		{	
		    e.printStackTrace();
		}
		return topicSet;
	}
	
	private Consumer<String, String> createConsumer(String serverid, int chunckSize, String table) {
    	final Properties props = new Properties();
    	
    	props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,Context.getAdministrationService().getGlobalProperty(BahmniSyncMasterConstants.KAFKA_URL_GLOBAL_PROPERTY_NAME));  
    	props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());  
    	props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());  
    	props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,serverid);  
    	props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
    	props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(chunckSize));
    	props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    	
    	String dbName = Context.getAdministrationService().getGlobalProperty(BahmniSyncMasterConstants.DATABASE_SERVER_NAME);
		String schemaName = Context.getAdministrationService().getGlobalProperty(BahmniSyncMasterConstants.OPENMRS_SCHEME_NAME);

    	// Create the consumer using props.
        KafkaConsumer<String,String> consumer= new KafkaConsumer<String,String>(props);  
        
        String topicPrefix = dbName + "." + schemaName + ".";
		table = topicPrefix + table;
		table = table.replace(",", "," + topicPrefix);

		String[] array = table.split(",");
		Set<String> topicSet = getTopicSet();
		List<String> arrayList = new ArrayList<String>();
		
		for(String arr: array){
			if(topicSet.contains(arr))
				arrayList.add(arr);
		}
		
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
	
	 public String getValue(String command, Connection con) {
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
		
		
		@Transactional
	    @Authorized(BahmniSyncMasterConstants.MANAGE_BAHMNI_SYNC_PRIVILEGE)
		public void cleanLog() {
			
			sessionFactory.getCurrentSession().doWork(new Work() {
				
				public void execute(Connection con) throws SQLException {
					
					Date date = Calendar.getInstance().getTime();  
					DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");  
					String strDate = dateFormat.format(date);  
					
					String filename = System.getProperty("user.dir") + File.separator + strDate + "-BahmniSyncLog";
					
					System.out.println("------"+filename+"------");
					
					FileWriter fw = null;
					
					try {
					
						fw = new FileWriter(filename + ".csv");
												
						Statement st = con.createStatement();
						String query = "select * from bahmnisyncmaster_log";
						ResultSet rs = st.executeQuery(query);
						
						int cols = rs.getMetaData().getColumnCount();
	
				         for(int i = 1; i <= cols; i ++){
				            fw.append(rs.getMetaData().getColumnLabel(i));
				            if(i < cols) fw.append(',');
				            else fw.append('\n');
				         }
	
				         while (rs.next()) {
	
				            for(int i = 1; i <= cols; i ++){
				            	if(rs.getString(i) == null)
				            		fw.append(rs.getString(i));
				            	else	
					            	fw.append(rs.getString(i).replace(",", "  "));
				            	
				                if(i < cols) fw.append(',');
				            }
				            fw.append('\n');
				            
				            int id = rs.getInt("bahmnisync_log_id");
				            System.out.println(id);
				            
				            DatabaseUtil.runCommand(CommandType.DELETE, "delete from bahmnisyncmaster_log where bahmnisync_log_id = " + id, con);
				        }
				       
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} finally {
						
						 try {
							fw.flush();
						    fw.close();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}				
				
			});	
			
		}

}
