package org.openmrs.module.bahmnisyncworker.util;

import java.util.HashSet;
import java.util.Set;

public class BahmniSyncWorkerConstants {
	
	//module id or name
	public static final String MODULE_ID = "bahmnisyncworker";
	
	/* Global Properties */
	public static final String WORKER_NODE_ID_GLOBAL_PROPERTY_NAME = MODULE_ID + ".worker.node.id";
	
	public static final String MASTER_URL_GLOBAL_PROPERTY_NAME = MODULE_ID + ".master.url";
	
	public static final String KAFKA_URL_GLOBAL_PROPERTY_NAME = MODULE_ID + ".kafka.url";
		
	public static final String CHUNK_SIZE_GLOBAL_PROPERTY_NAME = MODULE_ID + ".sync.chunk.size";
	
	public static final String DATABASE_SERVER_NAME = MODULE_ID + ".database.server.name";
	
	public static final String OPENMRS_SCHEME_NAME = MODULE_ID + ".openmrs.schema.name";
	
	public static final String MASTER_NODE_USER = MODULE_ID + ".master.node.user";
	
	public static final String MASTER_NODE_PASSWORD = MODULE_ID + ".master.node.password";
	
	/* Privileges*/
	public static final String MANAGE_BAHMNI_SYNC_PRIVILEGE = "Manage Bahmni Sync";
}
