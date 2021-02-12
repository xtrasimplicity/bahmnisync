package com.ihsinformatics.bahmnisync_server.controller;

import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ihsinformatics.bahmnisync_server.debezium.DebeziumObject;
import com.ihsinformatics.bahmnisync_server.debezium.DebeziumService;
import com.ihsinformatics.bahmnisync_server.debezium.consumer.BahmniSyncServerDataSync;


@RestController
public class DebeziumController {
	
	@PostMapping(path = "/debeziumObjects", consumes = "application/json", produces = "application/json")
	public ResponseEntity postDebeziumObjects(@RequestBody ArrayList<Map<String, Object>> debeziumObjects) throws IOException, ParseException {
				
		for(Map<String, Object> obj : debeziumObjects){
			DebeziumObject debeziumObject = DebeziumService.getDebziumObjectFromMap(obj);
			DebeziumService.executeDebeziumObject(debeziumObject);
		}
	
		return ResponseEntity.noContent().build();
	}
	
	@RequestMapping(value = {"/debeziumObjects", "/debeziumObjects/{serverid}/{chunksize}"})
    public String getDebeziumObjects(@PathVariable(required = true) String serverid, @PathVariable(required = true) Integer chunksize) throws Exception {
		
		if(chunksize == null)
			chunksize = 2;
			
		ArrayList<JSONObject> s = BahmniSyncServerDataSync.syncData(serverid, chunksize);
		System.out.println(s.size());
		return s.toString();
        
    }
	
}
