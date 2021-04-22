/**
 * The contents of this file are subject to the OpenMRS Public License
 * Version 1.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://license.openmrs.org
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * Copyright (C) OpenMRS, LLC.  All Rights Reserved.
 */
package org.openmrs.module.bahmnisyncmaster.web.controller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.openmrs.GlobalProperty;
import org.openmrs.Patient;
import org.openmrs.Person;
import org.openmrs.api.AdministrationService;
import org.openmrs.api.context.Context;
import org.openmrs.module.bahmnisyncmaster.BahmniSyncMasterLog;
import org.openmrs.module.bahmnisyncmaster.service.DataPushService;
import org.openmrs.module.bahmnisyncmaster.util.BahmniSyncMasterConstants;
import org.openmrs.web.WebConstants;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.WebRequest;

/**
 * This class configured as controller using annotation and mapped with the URL of
 * 'module/basicmodule/basicmoduleLink.form'.
 */
@Controller
@RequestMapping(value = "/module/bahmnisyncmaster/conflict2.form")
public class MasterConflict2Controller {
	
	@Autowired
	DataPushService dataPullService;
	
	/** Logger for this class and subclasses */
	protected final Log log = LogFactory.getLog(getClass());
	
	/** Success form view name */
	private final String SUCCESS_FORM_VIEW = "/module/bahmnisyncmaster/conflict2";
	
	/**
	 * Initially called after the formBackingObject method to get the landing form name
	 * 
	 * @return String form view name
	 * @throws IOException 
	 * @throws JsonMappingException 
	 * @throws JsonGenerationException 
	 */
	@RequestMapping(method = RequestMethod.GET)
	public String showForm() {
		return SUCCESS_FORM_VIEW;
	}
	
	@ModelAttribute("mastersyncconflicts")
	public String getModel() throws JsonGenerationException, JsonMappingException, IOException {
		
		List<BahmniSyncMasterLog> logs = dataPullService.getConflictBahmniSyncMasterLog();
		
		List<BahmniSyncMasterLog> newLogs = new ArrayList();
		for(BahmniSyncMasterLog log: logs){
			String master = log.getMasterData();
			String worker = log.getWorkerData();
						
			Map<String, String> masterMap = convertWithStream(master);
			Map<String, String> workerMap = convertWithStream(worker);
			
			String conflictingData = "";
			for (Map.Entry<String,String> entry : masterMap.entrySet()) {
				if(!entry.getValue().equals(workerMap.get(entry.getKey()))){
					String list = entry.getKey() + ": ";
					if(log.getMessage().contains("master"))
						list = list + "<b>" + entry.getValue() + "</b> , ";
					else	
						list = list + entry.getValue() + " , ";
					
					if(log.getMessage().contains("worker"))
						list = list + "<b>" + workerMap.get(entry.getKey())  + "</b> ";
					else	
						list = list + workerMap.get(entry.getKey()) ;
					 
					conflictingData = conflictingData + list + "<br/>";
				}
			}
	           
			log.setMasterData(conflictingData);
			
			if(log.getTable().contains("concept")){
				log.setStatus(masterMap.get("concept_id"));
			} else if(log.getTable().contains("patient")){
				/*Patient patient = Context.getPatientService().getPatientByUuid(masterMap.get("uuid"));
				String patientString = patient.getGivenName() + " " + patient.getFamilyName() + "("+ patient.getPatientIdentifier() +")";
				log.setStatus(patientString);*/
			} else if(log.getTable().contains("person")){
				
				Patient patient = Context.getPatientService().getPatientByUuid(masterMap.get("uuid"));
				System.out.println(patient);
				if(patient == null){
					
					Person person = Context.getPersonService().getPersonByUuid(masterMap.get("uuid"));
					System.out.println(person);
					System.out.println("HERE!!!");
					/*String personString = person.getGivenName() + " " + person.getFamilyName() + "(" + person.getPersonId() +")";
					log.setStatus(personString);*/
				}
				else {
					System.out.println("NOT HERE!!!");
					/*String patientString = patient.getGivenName() + " " + patient.getFamilyName() + "("+ patient.getPatientIdentifier() +")";
					log.setStatus(patientString);*/
				}
			} else if(log.getTable().equals("encounter") || log.getTable().equals("obs")){
				log.setStatus(masterMap.get("encounter_id"));
			} else {
				log.setStatus("-");
			}
			
			newLogs.add(log);
		}
		
		ObjectMapper mapper = new ObjectMapper();
		return mapper.writeValueAsString(logs);
				
	}
	
	public Map<String, String> convertWithStream(String mapAsString) {
		mapAsString = mapAsString.replace("{","");
		mapAsString = mapAsString.replace("}","");
		Map<String, String> map = new HashMap<>();
	    String[] splitArray = mapAsString.split(",");
	    for(String str : splitArray){
	    	String[] mapArray = str.split("=");
	    	if(mapArray.length == 2)
	    		map.put(mapArray[0],mapArray[1]);
	    }
	    return map;
	}
	
}
