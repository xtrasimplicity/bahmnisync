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
package org.openmrs.module.bahmnisyncmaster.web.resource;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;
import org.openmrs.GlobalProperty;
import org.openmrs.api.APIException;
import org.openmrs.api.AdministrationService;
import org.openmrs.api.context.Context;
import org.openmrs.module.bahmnisyncmaster.service.DataPullService;
import org.openmrs.module.bahmnisyncmaster.service.DataPushService;
import org.openmrs.module.bahmnisyncmaster.util.BahmniSyncMasterConstants;
import org.openmrs.web.WebConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.validation.Errors;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.WebRequest;

import org.springframework.ui.Model;

import org.openmrs.module.webservices.rest.web.RestConstants;


@Controller
@RequestMapping(value = "/rest/" + RestConstants.VERSION_1 + "/bahmnisync")
public class MasterSyncController {
	
	/** Logger for this class and subclasses */
	protected final Log log = LogFactory.getLog(getClass());
	
	@Autowired
	DataPushService dataPushService;
	
	@Autowired
	DataPullService dataPullService;
	
	@RequestMapping(value = "/debeziumObjects", method = RequestMethod.POST)
	@ResponseBody
	public String postDebeziumObjects(HttpServletRequest request,
            @RequestBody ArrayList<Map<String, Object>> debeziumObjects) throws Exception {

		  for(Map<String, Object> obj : debeziumObjects){
			  dataPushService.startDataPush(obj);
		  }
	
		return "DONE!";
	}
	
	@RequestMapping(value = "/debeziumObjects/{serverid}/{table}/{chunksize}", method = RequestMethod.GET)
	@ResponseBody
    public String getDebeziumObjects(HttpServletRequest request, @PathVariable(value="serverid") String serverid, @PathVariable(value="table") String table, @PathVariable(value="chunksize") Integer chunksize) throws Exception {
			
		ArrayList<JSONObject> s = dataPullService.startDataPull(serverid, chunksize,table);        
		return s.toString();
        
    }

}
