package org.openmrs.module.bahmnisyncmaster.service.Impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.hibernate.SessionFactory;
import org.hibernate.jdbc.Work;
import org.openmrs.api.APIException;
import org.openmrs.api.db.hibernate.DbSessionFactory;
import org.openmrs.api.impl.BaseOpenmrsService;
import org.openmrs.module.bahmnisyncmaster.BahmniSyncMasterLog;
import org.openmrs.module.bahmnisyncmaster.debezium.DebeziumObject;
import org.openmrs.module.bahmnisyncmaster.service.MasterLogService;
import org.openmrs.module.bahmnisyncmaster.util.CommandType;
import org.openmrs.module.bahmnisyncmaster.util.DatabaseUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class MasterLogServiceImpl extends BaseOpenmrsService implements MasterLogService {

	@Autowired
	private SessionFactory sessionFactory;

	public SessionFactory getSessionFactory() {
		return sessionFactory;
	}
	
	@Override
	public BahmniSyncMasterLog saveBahmniSyncMasterLog(BahmniSyncMasterLog log) throws APIException {
		sessionFactory.getCurrentSession().saveOrUpdate(log);
		return log;
	}

	
}
