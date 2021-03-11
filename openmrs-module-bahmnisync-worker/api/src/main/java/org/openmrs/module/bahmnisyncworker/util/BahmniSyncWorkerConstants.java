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
	
	public static final String SYNC_TABLE_GLOBAL_PROPERTY_NAME = MODULE_ID + ".sync.table";
	
	public static final String CHUNK_SIZE_GLOBAL_PROPERTY_NAME = MODULE_ID + ".sync.chunk.size";
	
	public static final String DEBEZIUM_CONNECT_URL_GLOBAL_PROPERTY_NAME = MODULE_ID + ".debezium.connect.url";
}
