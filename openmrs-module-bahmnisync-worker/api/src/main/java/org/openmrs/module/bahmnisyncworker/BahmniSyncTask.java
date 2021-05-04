package org.openmrs.module.bahmnisyncworker;

import java.io.IOException;

import org.openmrs.api.context.Context;
import org.openmrs.module.bahmnisyncworker.util.HttpConnection;
import org.openmrs.scheduler.tasks.AbstractTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BahmniSyncTask extends AbstractTask {
	
	private static final Logger log = LoggerFactory.getLogger(BahmniSyncTask.class);


	@Override
	public void execute() {
		
		if (!isExecuting) {
			try {
				String v = "http://localhost:8080/openmrs/ws/rest/v1/bahmnisync/startsyncprocess";
			}
			catch (Exception e) {
				log.error("Error while auto bahmni sync process:", e);
			}
			finally {
				stopExecuting();
			}
		}
		
	}

}
