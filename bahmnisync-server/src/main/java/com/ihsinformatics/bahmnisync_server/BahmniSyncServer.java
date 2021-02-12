package com.ihsinformatics.bahmnisync_server;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.ihsinformatics.bahmnisync_server.util.DatabaseUtil;

/**
 * Hello world!
 *
 */
@SpringBootApplication
public class BahmniSyncServer 
{
	private static final Logger log = Logger.getLogger(Class.class.getName());
	public static String resourceFile = "bahmnisyncserver.properties";
	public static Properties appProps;
	public static DatabaseUtil dbUtil;
	
	public static Logger ERROR_LOGGER = Logger.getLogger("error");
	public static Logger CONFLICTS_LOGGER = Logger.getLogger("conflicts");
	static FileHandler fh1;
	static FileHandler fh2;
	
    public static void main( String[] args ) throws SecurityException, IOException
    {
    	
    	fh1 = new FileHandler("Error.log");  
		ERROR_LOGGER.addHandler(fh1);
        SimpleFormatter formatter = new SimpleFormatter();  
        fh1.setFormatter(formatter);
        
        fh2 = new FileHandler("Conflicts.log");  
		CONFLICTS_LOGGER.addHandler(fh2);
        fh2.setFormatter(formatter);
    	
        readProperties();
    	initializeDatabase();
        
    	SpringApplication.run(BahmniSyncServer.class, args);
   
    	
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
