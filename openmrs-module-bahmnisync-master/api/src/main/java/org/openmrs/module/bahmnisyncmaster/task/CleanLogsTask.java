package org.openmrs.module.bahmnisyncmaster.task;

import java.io.IOException;

import org.openmrs.api.context.Context;
import org.openmrs.scheduler.tasks.AbstractTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CleanLogsTask extends AbstractTask {
	
	private static final Logger log = LoggerFactory.getLogger(CleanLogsTask.class);


	@Override
	public void execute() {
		
		if (!isExecuting) {
			try {
				String v = "http://localhost:8080/openmrs/ws/rest/v1/bahmnisync/cleanlog";
			}
			catch (Exception e) {
				log.error("Error while auto rebuild search index:", e);
			}
			finally {
				stopExecuting();
			}
		}
		
	}

}
