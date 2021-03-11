package org.openmrs.module.bahmnisyncmaster.service;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

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
import org.openmrs.api.APIException;
import org.openmrs.api.context.Context;
import org.openmrs.api.db.hibernate.DbSessionFactory;
import org.openmrs.module.bahmnisyncmaster.util.BahmniSyncMasterConstants;
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
	public ArrayList<JSONObject> startDataPull(final String serverid, final int chunckSize) {

		sessionFactory.getCurrentSession().doWork(new Work() {
			
			public void execute(Connection connection) {
				
				try {
					startConsumer(connection, serverid, chunckSize);
				} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
		return postObjects;
	}
	
	private void startConsumer(Connection con, String serverid, int chunckSize) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException{
		final Consumer<String, String> consumer = createConsumer( serverid,  chunckSize);
        ConsumerRecords<String,String> records = consumer.poll(10000);
        
        for(ConsumerRecord<String,String> record: records){   
        	
        	JSONObject postJB = new JSONObject();
        	
        	if(record.value() == null) 
        		continue;
        	
        	JSONObject json = new JSONObject(record.value());  
            JSONObject payload = json.getJSONObject("payload");
            String db = payload.getJSONObject("source").getString("db");
            String table = payload.getJSONObject("source").getString("table");
            String pkColumn = getPrimaryKey(table, con);
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
                                	                
                for (Object[] fk : foreignkeys){
                	
                  JSONObject fkJSONObject = new JSONObject();
                  
                  String colName = String.valueOf(fk[1]);
                  String referencedTableNme = String.valueOf(fk[3]);
                  String referencedColName = String.valueOf(fk[4]);
                  
                  String dt = getColumnDataType(json.getJSONObject("schema"),colName);
                  if(dt.equals(DataType.INT32.label)){
                	  Object fkValue = payload.getJSONObject("after").get(colName);
                	  if(!fkValue.equals(null))	{
                		  String fktableReference = "SELECT uuid " + 
  		  					"from "+ db + "." + referencedTableNme + " " +
  		  					"where " + referencedColName + " = " + fkValue;
                	  	String uuid = getValue(fktableReference, con);
                	  	fkJSONObject.put("COLUMN_NAME", colName);
                        fkJSONObject.put("REFERENCED_TABLE_NAME", referencedTableNme);
                        fkJSONObject.put("REFERENCED_COLUMN_NAME", referencedColName);
                	  	fkJSONObject.put("REFERENCED_UUID", uuid);
                	  	jsonDataAfter.put(colName, uuid);
                	  	fkJsonArray.put(fkJSONObject);
                	  }
                  }
                
               }
                  
                JSONArray fieldsArray = getJSONArrayFromSchema(schema,"after");
                for(int j=0; j<fieldsArray.length(); j++){
            		JSONObject jObj = fieldsArray.getJSONObject(j);
        			String cName = jObj.getString("field");
        			
        			String datatype = getColumnDataType(schema,cName);
        			if(datatype.equals(DataType.INT32.label) && cName.equals(pkColumn))
        				jsonDataAfter.put(cName, "PK");        			
        			
        			if(datatype.equals(DataType.INT64.label) && !jsonDataAfter.get(cName).equals(null)){
    	                String strDate = convertTimeWithTimeZome(jsonDataAfter.getLong(cName));
    	                jsonDataAfter.put(cName, strDate);
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
            		jsonDataBefore, pkColumn, fkJsonArray);
            
            postJB.put("fk", fkJsonArray);
            postJB.put("data", jsonDataAfter);
            postJB.put("op", payload.getString("op"));
            postJB.put("db", payload.getJSONObject("source").getString("db"));
            postJB.put("table", payload.getJSONObject("source").getString("table"));
            postJB.put("pk", pkColumn);
            postJB.put("query", queryForPost);
            
            
            postObjects.add(postJB);
    
    	}
    	
    	consumer.commitAsync();
    	consumer.close();  
	}
	
	private Consumer<String, String> createConsumer(String serverid, int chunckSize) {
    	final Properties props = new Properties();
    	
    	props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,Context.getAdministrationService().getGlobalProperty(BahmniSyncMasterConstants.KAFKA_URL_GLOBAL_PROPERTY_NAME));  
    	props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());  
    	props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());  
    	props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,serverid);  
    	props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
    	props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(chunckSize));

    	// Create the consumer using props.
        KafkaConsumer<String,String> consumer= new KafkaConsumer<String,String>(props);  

    	// Subscribe to the topic
        String[] array = Context.getAdministrationService().getGlobalProperty(BahmniSyncMasterConstants.SYNC_TABLE_GLOBAL_PROPERTY_NAME).split(",");
        consumer.subscribe(Arrays.asList(array));  
    	return consumer;
     }
	
	public String getPrimaryKey(String tableName, Connection con) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException{
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
			Calendar cal = Calendar.getInstance();
			cal.setTimeZone(TimeZone.getTimeZone("UTC"));
			cal.setTimeInMillis(time);
			StringBuilder stringDate = new StringBuilder(cal.get(Calendar.YEAR) + "-");
			
			if (cal.get(Calendar.MONTH) < 10)
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
				
				if ((!pkColumn.equalsIgnoreCase(cName)) && (!data.get(cName).equals(null))) {
					fieldsString.append(cName + ",");
					
					if (isForeignKey(cName, fkJsonArray)) {
						valuesString.append("<" + cName + ">" + ",");
						continue;
					}
					
					String datatype = getColumnDataType(schema, cName);
					if (datatype.equals(DataType.INT32.label))
						valuesString.append(data.getInt(cName) + ",");
					else if (datatype.equals(DataType.STRING.label))
						valuesString.append("'" + data.getString(cName) + "',");
					else if (datatype.equals(DataType.INT64.label)) {
						String strDate = "";
						if (data.get(cName) instanceof Long)
							strDate = convertTimeWithTimeZome(data.getLong(cName));
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
						updateQuery.append(data.getInt(cName) + ",");
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
			
			if (datatype.equals(DataType.INT32.label))
				updateQuery.append(" where uuid = '" + data.getString("uuid") + "';");
			else if (datatype.equals(DataType.STRING.label))
				updateQuery.append(" where" + pkColumn + " = '" + data.getString(pkColumn) + "';");
			
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

}
