package com.ihsinformatics.bahmnisyncclient;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONArray;
import org.json.JSONObject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ihsinformatics.bahmnisyncclient.connection.HTTPConnection;
import com.ihsinformatics.bahmnisyncclient.debezium.DebeziumObject;
import com.ihsinformatics.bahmnisyncclient.debezium.DebeziumService;
import com.ihsinformatics.bahmnisyncclient.util.DataType;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.RequestBody;

public class BahmniSyncClientDataPull {
	
	public static String TOPICS;
	public static String BOOTSTRAP_SERVERS;
	public static String MASTER_URL;
	public static String GROUP_ID;
	public static String MAX_POLL_SIZE;
	
	static void pullData() throws Exception {
		
		BOOTSTRAP_SERVERS = BahmniSyncClient.appProps.getProperty("kafka.bootstrap.server.url");
    	TOPICS =  BahmniSyncClient.appProps.getProperty("kafka.topics");
    	MASTER_URL = BahmniSyncClient.appProps.getProperty("master.url");
    	GROUP_ID = BahmniSyncClient.appProps.getProperty("kafka.consumer.groupid");
    	MAX_POLL_SIZE = BahmniSyncClient.appProps.getProperty("kafka.consumer.chunk.size");

    	JSONArray jsonArray = null;
    	do{
	    	String s = HTTPConnection.doGetRequest(MASTER_URL+"/debeziumObjects/"+GROUP_ID+"/"+MAX_POLL_SIZE);
	    	jsonArray = new JSONArray(s); 
	    		    	
	    	for(Object obj : jsonArray){
	    		JSONObject jb = new JSONObject(obj.toString());
				DebeziumObject debeziumObject = DebeziumService.getDebziumObjectFromJSON(jb);
				DebeziumService.executeDebeziumObject(debeziumObject);
			}
    	}while(jsonArray.length() == 2);
    	
		
    }
	
	
}
