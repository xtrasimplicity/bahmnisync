package org.openmrs.module.bahmnisyncmaster.util;

import java.util.HashSet;
import java.util.Set;

public class BahmniSyncMasterConstants {
	
	//module id or name
	public static final String MODULE_ID = "bahmnisyncmaster";
	
	/* Global Properties */
	
	public static final String KAFKA_URL_GLOBAL_PROPERTY_NAME = MODULE_ID + ".kafka.url";
	
	public static final String SYNC_TABLE_GLOBAL_PROPERTY_NAME = MODULE_ID + ".sync.table";
	
	public static final String DEBEZIUM_CONNECT_URL_GLOBAL_PROPERTY_NAME = MODULE_ID + ".debezium.connect.url";
}
