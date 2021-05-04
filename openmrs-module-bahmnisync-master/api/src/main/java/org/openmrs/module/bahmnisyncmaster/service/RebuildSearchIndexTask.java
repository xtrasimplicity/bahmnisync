package org.openmrs.module.bahmnisyncmaster.service;

import java.io.IOException;

import org.openmrs.api.context.Context;
import org.openmrs.scheduler.tasks.AbstractTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RebuildSearchIndexTask extends AbstractTask {
	
	private static final Logger log = LoggerFactory.getLogger(RebuildSearchIndexTask.class);


	@Override
	public void execute() {
		
		if (!isExecuting) {
			try {
				Context.updateSearchIndex();
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
