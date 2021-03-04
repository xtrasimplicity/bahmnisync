package org.openmrs.module.bahmnisyncmaster.service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.hibernate.jdbc.Work;
import org.openmrs.api.APIException;
import org.openmrs.api.db.hibernate.DbSessionFactory;
import org.openmrs.module.bahmnisyncmaster.BahmniSyncMasterLog;
import org.openmrs.module.bahmnisyncmaster.debezium.DebeziumObject;
import org.openmrs.module.bahmnisyncmaster.util.CommandType;
import org.openmrs.module.bahmnisyncmaster.util.DatabaseUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public interface MasterLogService  {

	@Transactional
	BahmniSyncMasterLog saveBahmniSyncMasterLog(BahmniSyncMasterLog log)throws APIException;

}
