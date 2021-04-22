package org.openmrs.module.bahmnisyncmaster.util;

import java.util.HashSet;
import java.util.Set;

public class BahmniSyncMasterConstants {
	
	//module id or name
	public static final String MODULE_ID = "bahmnisyncmaster";
	
	/* Global Properties */
	
	public static final String KAFKA_URL_GLOBAL_PROPERTY_NAME = MODULE_ID + ".kafka.url";
	
	public static final String DATABASE_SERVER_NAME = MODULE_ID + ".database.server.name";
	
	public static final String OPENMRS_SCHEME_NAME = MODULE_ID + ".openmrs.schema.name";
	
	
	/* Privileges*/
	public static final String MANAGE_BAHMNI_SYNC_PRIVILEGE = "Manage Bahmni Sync";
}
