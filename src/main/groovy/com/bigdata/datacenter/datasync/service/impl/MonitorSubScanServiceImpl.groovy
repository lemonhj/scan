package com.bigdata.datacenter.datasync.service.impl

import com.bigdata.datacenter.datasync.core.data.MongoTemplatePool
import com.bigdata.datacenter.datasync.quartz.SubjectServiceJobNew
import com.bigdata.datacenter.datasync.service.ScanService
import com.bigdata.datacenter.datasync.service.SubjectStatusService
import com.bigdata.datacenter.datasync.service.TxtAndRrpStatusService
import com.bigdata.datacenter.datasync.service.XmlRawSQLService
import com.bigdata.datacenter.datasync.utils.DateUtils
import com.bigdata.datacenter.datasync.utils.ESearchHelper
import com.bigdata.datacenter.datasync.utils.EntityUtil
import com.bigdata.datacenter.datasync.utils.Md5
import com.bigdata.datacenter.datasync.utils.MongoUtils
import com.bigdata.datacenter.datasync.utils.PropertiesUtil
import com.bigdata.datacenter.datasync.utils.StringUtils
import com.bigdata.datacenter.datasync.utils.constants.MongoDBConfigConstants
import com.bigdata.datacenter.datasync.utils.constants.EsConfigConstants
import com.bigdata.datacenter.datasync.model.mongodb.ParamObj
import com.bigdata.datacenter.datasync.model.mongodb.SubjectStatus
import com.bigdata.datacenter.metadata.service.RawSQLService
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.collections.map.HashedMap
import org.apache.log4j.Logger
import org.quartz.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.stereotype.Service

import com.bigdata.datacenter.metadata.service.ResultHandler

import java.sql.Clob
import java.sql.SQLException
import java.text.ParseException
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

import com.bigdata.datacenter.metadata.data.EbeanServerPool
import com.avaje.ebean.EbeanServer
import com.avaje.ebean.SqlQuery

import java.util.function.Consumer

import com.avaje.ebean.SqlRow

import static org.springframework.data.mongodb.core.query.Criteria.where
import static org.springframework.data.mongodb.core.query.Query.query
import org.springframework.data.mongodb.core.query.Update


/**
 * 监控数据源扫描
 */  
@Service(value = "monitor-sub-scan")
class MonitorSubScanServiceImpl  {
    private static final Logger logger = Logger.getLogger(MonitorSubScanServiceImpl.class)
 
    @Autowired
    MongoTemplatePool mongoTemplatePool
  
    //邮件服务
    @Autowired
    MailService mailService

	private static Map<String,Date> subdataMap = new HashedMap<String,Date>();
  
	
	/***
	 * 监控状态
	 */
	public void monitorSub(){
		logger.info("监控数据源状态开始")
		long startTime = System.currentTimeMillis();

		MongoOperations mongoOps = mongoTemplatePool.getByName(MongoDBConfigConstants.DS_STATUS_DB)
		List<SubjectStatus> subjects = mongoOps.find(query(where("DS_STAS").in(0,1)), SubjectStatus.class)
		Date now = new Date();
		for(SubjectStatus sub : subjects){
				/**
				 * 数据源状态为0
				 * 1、如果last_upd_time 大于 ds_upd_time, 并且ds_upd_time 与 当前时间相差大于5小时
				 * 2、如果last_upd_time 小于 ds_upd_time, 并且last_upd_time 与 当前时间相差大于5小时
				 * 1、如果last_upd_time 为null , 并且ds_upd_time 与 当前时间相差大于5小时
				 */
			   if (sub.DS_STAS==0 &&
					   ((sub.LAST_UPD_TIME !=null && sub.LAST_UPD_TIME.after(sub.DS_UPD_TIME)
							   && (sub.DS_UPD_TIME.getTime() - now.getTime() > 5*60*60*1000))
						|| (sub.LAST_UPD_TIME !=null && sub.LAST_UPD_TIME.before(sub.DS_UPD_TIME)
							   && (sub.LAST_UPD_TIME.getTime() - now.getTime() > 5*60*60*1000) )
					   || (sub.LAST_UPD_TIME ==null && (sub.DS_UPD_TIME.getTime() - now.getTime() > 5*60*60*1000) ))) {
				  sub.DS_STAS = 2;
				  Update update =new Update()
				  update.set("DS_STAS", 2)
				  mongoOps.updateFirst(query(where("DS_ID").is(sub.DS_ID)), update, SubjectStatus.class)
				   sendErrMsg(sub);

			   }else if(sub.DS_STAS==1) {
					  if (sub.DS_UPD_TIME.getTime() - now.getTime() > 5*60*60*1000) {
						  sub.DS_STAS = 2;
						  Update update =new Update()
						  update.set("DS_STAS", 2)
						  mongoOps.updateFirst(query(where("DS_ID").is(sub.DS_ID)), update, SubjectStatus.class)
						  sendErrMsg(sub);
					  }
			   }
		}
		long endTime = System.currentTimeMillis();
		logger.info("监控数据源状态结束,共耗时：" + (endTime - startTime) + "ms");
	}

	/**
	 * 发送错误信息邮件，一天只发送一次
	 * @param sub
	 */
	private void sendErrMsg(SubjectStatus sub){
		if(!subdataMap.containsKey(sub.DS_NAME)){
			subdataMap.put(sub.DS_NAME,new Date())
			mailService.sendMail("数据源["+sub.DS_NAME+"]扫描不正常","数据源["+sub.DS_NAME+"]扫描不正常,请及时进行处理! 数据源信息为："+sub.toString())
		}else{
			Date date = subdataMap.get(sub.DS_NAME);
			Date lastDate = DateUtils.string2Date(DateUtils.date2String(date, DateUtils.FORMAT_DATE), DateUtils.FORMAT_DATE)
			Date nowDate = DateUtils.string2Date(DateUtils.date2String(new Date(), DateUtils.FORMAT_DATE), DateUtils.FORMAT_DATE)
			if(lastDate < nowDate){
				mailService.sendMail("数据源["+sub.DS_NAME+"]扫描不正常","数据源["+sub.DS_NAME+"]扫描不正常,请及时进行处理! 数据源信息为："+sub.toString())
			}
		}

	}

	
  


   

}
