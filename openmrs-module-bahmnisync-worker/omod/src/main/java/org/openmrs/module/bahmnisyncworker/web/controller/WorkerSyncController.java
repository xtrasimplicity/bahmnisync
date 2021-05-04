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
package org.openmrs.module.bahmnisyncworker.web.controller;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.openmrs.GlobalProperty;
import org.openmrs.api.APIException;
import org.openmrs.api.AdministrationService;
import org.openmrs.api.context.Context;
import org.openmrs.module.bahmnisyncworker.BahmniSyncWorkerLog;
import org.openmrs.module.bahmnisyncworker.BahmniSyncWorkerService;
import org.openmrs.module.bahmnisyncworker.util.BahmniSyncWorkerConstants;
import org.openmrs.web.WebConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.validation.Errors;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.WebRequest;
import org.springframework.ui.Model;

/**
 * This class configured as controller using annotation and mapped with the URL of
 * 'module/basicmodule/basicmoduleLink.form'.
 */
@Controller
public class WorkerSyncController {
	
	/** Logger for this class and subclasses */
	protected final Log log = LogFactory.getLog(getClass());
	
	@Autowired
	BahmniSyncWorkerService syncWorkerService;
	
	/** Success form view name */
	private final String SUCCESS_FORM_VIEW = "/module/bahmnisyncworker/sync";
	
	/**
	 * Initially called after the formBackingObject method to get the landing form name
	 * 
	 * @return String form view name
	 */
	@RequestMapping(method = RequestMethod.GET, value = "/module/bahmnisyncworker/sync.form")
	public String showForm(final ModelMap modelMap) {
		
		List<BahmniSyncWorkerLog> logs = syncWorkerService.getAllBahmniSyncWorkerLog();
		if(logs.size() == 0){
		
			modelMap.addAttribute("syncDate", "-");
			modelMap.addAttribute("syncStatus", "-");
			
		} else {
			
			BahmniSyncWorkerLog log = logs.get(0);
			
			modelMap.addAttribute("syncDate", log.getLogDateTime());
			modelMap.addAttribute("syncStatus", log.getStatus());
			
			if(!log.getStatus().contains("Success"))
				modelMap.addAttribute("syncMessage", log.getMessage());
		    else 
				modelMap.addAttribute("syncMessage", "");

		}
		return SUCCESS_FORM_VIEW;
	}
	
	@RequestMapping(method = RequestMethod.GET, value = "/module/bahmnisyncworker/startPushData.form")
	public @ResponseBody
	Map<String, Boolean> startDataPush() {
		
		Map<String, Boolean> results = new HashMap<>();
		Boolean ready = false;	
		try{

			ready = syncWorkerService.startDataPush();
			results.put("ready",ready);
					
		}catch(Exception e){
			results.put("ready", false);
			log.error(e);
		}
		
		return results;
	}
	
	/**
	 * @should return return inProgress for status if a rebuildSearchIndex is not completed
	 * @should return success for status if a rebuildSearchIndex is completed successfully
	 * @should return error for status if a rebuildSearchIndex is not completed normally
	 * @return hashMap of String, String holds a key named "status" indicating the status of rebuild
	 *         search index
	 */
	@RequestMapping(method = RequestMethod.GET, value = "/module/bahmnisyncworker/startsyncstatus.form")
    public @ResponseBody Map<String, String> getStatus() {

        Map<String, String> results = new HashMap<>();
        results.put("status", "success");
        
        return results;
    }
	
	@RequestMapping(method = RequestMethod.GET, value = "/module/bahmnisyncworker/kafkaconnection.form")
    public @ResponseBody Map<String, Boolean> checkKafkaConnection() {
		
		Map<String, Boolean> results = new HashMap<>();
		Boolean ready = false;
		try {
			ready = syncWorkerService.checkKafkaConnection();
		} catch (IOException e1) {
			
			log.error(e1);
			ready = false;
		}
		
		results.put("ready", ready);
        return results;
	}
	
	@RequestMapping(method = RequestMethod.GET, value = "/module/bahmnisyncworker/syncready.form")
    public @ResponseBody Map<String, Boolean> isSyncReady() {

		Map<String, Boolean> results = new HashMap<>();		
		Boolean status = syncWorkerService.checkSyncReadyStatus();
        results.put("ready", status);
        return results;
    }
	
	@RequestMapping(method = RequestMethod.GET, value = "/module/bahmnisyncworker/checkconnection.form")
    public @ResponseBody Map<String, Boolean> checkConnection() throws IOException {
		
		Boolean status = syncWorkerService.checkMasterConnection();
        Map<String, Boolean> results = new HashMap<>();
        results.put("ready", status);
        return results;
    }
	
	@RequestMapping(method = RequestMethod.GET, value = "/module/bahmnisyncworker/startPullData.form")
	public @ResponseBody
	Map<String, Boolean> startDataPull() {
		
		Map<String, Boolean> results = new HashMap<>();
		Boolean ready = false;
				
		try{
			//ready = syncWorkerService.startDataPull();
			ready = true;
		}catch(Exception e){
			ready = false;
			log.error(e);
		}
		
		results.put("ready", ready);
		
		return results;
	}
	
}
