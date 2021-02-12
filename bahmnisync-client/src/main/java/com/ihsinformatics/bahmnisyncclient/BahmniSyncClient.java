package com.ihsinformatics.bahmnisyncclient;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONObject;

import com.ihsinformatics.bahmnisyncclient.util.DataType;
import com.ihsinformatics.bahmnisyncclient.util.DatabaseUtil;

import okhttp3.FormBody;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

/**
 * Hello world!
 *
 */
public class BahmniSyncClient 
{
	
	private static final Logger log = Logger.getLogger(Class.class.getName());
	public static String resourceFile = "bahmnisyncclient.properties";
	public static Properties appProps;
	public static DatabaseUtil dbUtil;
	
	
		
    public static void main( String[] args ) throws Exception
    {
    	readProperties();
    	initializeDatabase();
    	
    	System.out.println("-x-x-x-x- Data Push Started -x-x-x-x-");
    	BahmniSyncClientDataPush.pushData();
    	System.out.println("-x-x-x-x- Data Push Ended -x-x-x-x-");
    	
    	System.out.println("-x-x-x-x- Data Pull Started -x-x-x-x-");
    	BahmniSyncClientDataPull.pullData();
    	System.out.println("-x-x-x-x- Data Pull Ended -x-x-x-x-");
    	
    }
    
    public static void initializeDatabase(){
    	
    	dbUtil = new DatabaseUtil();
    	
    	String url = appProps.getProperty("local.connection.url");
		String driverName = appProps.getProperty("local.connection.driver.class");
		String db = appProps.getProperty("local.connection.database");
		String username = appProps.getProperty("local.connection.username");
		String password = appProps.getProperty("local.connection.password");
		
    	dbUtil.setConnection(url, db, driverName, username, password);
    	
    }
 
 	public static void readProperties() {
 		try {
 		    
 			InputStream propertiesInputStream = Thread.currentThread()
 				    .getContextClassLoader().getResourceAsStream(resourceFile);
 			  
 			appProps = new Properties();
 			appProps.load(propertiesInputStream);
 			
 			
 		} catch (Exception e) {
 			log.info("Exception: " + e.toString());
 			System.exit(-1);
 		}
 	}
    
}



