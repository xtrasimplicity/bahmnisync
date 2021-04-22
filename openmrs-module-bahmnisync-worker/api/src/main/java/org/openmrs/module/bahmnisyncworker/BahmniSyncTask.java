package org.openmrs.module.bahmnisyncworker;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Collection;
import java.util.Date;

import org.openmrs.Role;
import org.openmrs.User;
import org.openmrs.api.context.Context;
import org.openmrs.notification.Alert;
import org.openmrs.scheduler.tasks.AbstractTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Simple implementation to check if we have a connection to the internet.
 */
public class BahmniSyncTask extends AbstractTask {
	
	@Autowired
	BahmniSyncWorkerService syncWorkerService;
	
	/**
	 * Logger
	 */
	private static final Logger log = LoggerFactory.getLogger(BahmniSyncTask.class);
	
	/**
	 * @see org.openmrs.scheduler.tasks.AbstractTask#execute()
	 */
	@Override
	public void execute() {
		
		syncWorkerService.startDataPush();
		syncWorkerService.startDataPull();		
	}
}
