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
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.openmrs.Concept;
import org.openmrs.Encounter;
import org.openmrs.GlobalProperty;
import org.openmrs.Patient;
import org.openmrs.api.AdministrationService;
import org.openmrs.api.context.Context;
import org.openmrs.module.bahmnisyncmaster.BahmniSyncMasterLog;
import org.openmrs.module.bahmnisyncmaster.BahmniSyncMasterObsConflicts;
import org.openmrs.module.bahmnisyncmaster.BahmniSyncMasterSyncConflictDTO;
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
public class MasterConflictController {
	
	@Autowired
	DataPushService dataPullService;
	
	/** Logger for this class and subclasses */
	protected final Log log = LogFactory.getLog(getClass());
	
	/** Success form view name */
	private final String SUCCESS_FORM_VIEW = "/module/bahmnisyncmaster/obsconflict";
	
	/**
	 * Initially called after the formBackingObject method to get the landing form name
	 * 
	 * @return String form view name
	 */
	@RequestMapping(method = RequestMethod.GET, value = "/module/bahmnisyncmaster/obsconflict.form")
	public String showForm() {
		return SUCCESS_FORM_VIEW;
	}
	
	@ModelAttribute("mastersyncconflicts")
	public String getModel() throws JsonGenerationException, JsonMappingException, IOException {
		
		List<BahmniSyncMasterObsConflicts> logs = dataPullService.getObsConflictBahmniSyncMasterLog();
		
		List<BahmniSyncMasterSyncConflictDTO> dtoLogs = new ArrayList<>();
		for(BahmniSyncMasterObsConflicts log : logs){
			
			BahmniSyncMasterSyncConflictDTO dto = new BahmniSyncMasterSyncConflictDTO();
			dto.setBahmniSyncObsConflictId(log.getBahmniSyncObsConflictsId());
			dto.setWorkerId(log.getWorkerId());
			dto.setLogDateTime(log.getLogDateTime());
			
			Patient patient = Context.getPatientService().getPatient(log.getPatientId());
			if(patient != null){
				String patientString = patient.getGivenName() + " " + patient.getFamilyName() + "<i>("+ patient.getPatientIdentifier() +")</i>";
				dto.setPatient(patientString);
			}
			else
				dto.setPatient("-");
			
			Encounter enc = Context.getEncounterService().getEncounter(log.getEncounterId());
			if(enc != null){
				String encounterString = enc.getEncounterType().getName() + "<i>(" + enc.getEncounterId() + ")</i>" ;
				dto.setEncounter(encounterString);
			}
			else
				dto.setEncounter("-");
			
			Concept concept = Context.getConceptService().getConcept(log.getConceptId());
			if(concept != null){
				String conceptString = concept.getDisplayString() + "<i>(" + concept.getConceptId() + ")</i>" ;
				dto.setConcept(conceptString);
			}
			else
				dto.setConcept("-");
			
			String workerdata = log.getWorkerData();
			
			String dataType = log.getValueType();
			if(dataType.equals("concept")){
				Concept concept1 = Context.getConceptService().getConcept(workerdata);
				if(concept1 != null){
					String conceptString = concept1.getDisplayString() + "<i>(" + concept1.getConceptId() + ")</i>" ;
					dto.setWorkerData(conceptString);
				}
				else
					dto.setWorkerData("-");
			} else 
				dto.setWorkerData(workerdata);
			
			String serviceData = log.getMasterData();
			String[] datas = serviceData.split(",");
			
			String value = "";
			for(String data:datas){
				if(dataType.equals("concept")){
					Concept concept1 = Context.getConceptService().getConcept(data);
					if(concept1 != null){
						String conceptString = concept1.getDisplayString() + "<i>(" + concept1.getConceptId() + ")</i>" ;
						value = value + conceptString + "</br>";
					}
				} else 
					value = value + data + "</br>";
			}
			dto.setMasterData(value);
			
			dtoLogs.add(dto);
		}
		
		
		ObjectMapper mapper = new ObjectMapper();
		return mapper.writeValueAsString(dtoLogs);
		
	}
	
	@RequestMapping(method = RequestMethod.GET, value = "/module/bahmnisyncmaster/resolveObsConflict.form")
	public @ResponseBody String markConflictAsResolved(@RequestParam("id")Integer id) throws JsonGenerationException, JsonMappingException, IOException
	{
		dataPullService.deleteObsConflict(id);
		return "success";
	}
	
	
}
