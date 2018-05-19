package com.bigdata.datacenter.datasync.service.impl.mysql

import com.avaje.ebean.SqlRow
import com.bigdata.datacenter.datasync.model.mongodb.Subject
import com.bigdata.datacenter.datasync.service.impl.MailService
import com.bigdata.datacenter.datasync.utils.PropertiesUtil
import com.bigdata.datacenter.datasync.utils.StringUtils
import com.bigdata.datacenter.datasync.utils.constants.EsConfigConstants
import com.bigdata.datacenter.datasync.utils.constants.MongoDBConfigConstants
import com.bigdata.datacenter.metadata.service.ResultHandler
import org.apache.log4j.Logger

import java.sql.Clob
/**
 * 专题增量数据MYSQL版
 * Created by haiyangp on 2017/11/21.
 */
class MysqlSubjectIncrementalResultHandler implements ResultHandler{
    private static final Logger logger = Logger.getLogger(MysqlSubjectIncrementalResultHandler.class)
    def String dsNm
//    def MongoOperations subjectTemplate;
    def DBCommon_mysql subjectTemplate
    def List<Map<String, Object>> props
    def String opType
    def List<String> paramNames
    def MailService mailService
    def  List<Subject> allKeys = null

    MysqlSubjectIncrementalResultHandler(){}

    MysqlSubjectIncrementalResultHandler(DBCommon_mysql subjectTemplate, List<Map<String, Object>> props,
    String dsNm,List<String> paramNames,String opType){
        this.subjectTemplate = subjectTemplate;
        this.props = props;
        this.dsNm = dsNm;
        this.paramNames = paramNames;
        this.opType = opType;
        if(opType == MongoDBConfigConstants.UPD_TYPE){
            allKeys = new ArrayList<Subject>();
        }
    }

    MysqlSubjectIncrementalResultHandler(DBCommon_mysql subjectTemplate, List<Map<String, Object>> props,
    String dsNm,List<String> paramNames,String opType,MailService mailService){
        this.subjectTemplate = subjectTemplate;
        this.props = props;
        this.dsNm = dsNm;
        this.paramNames = paramNames;
        this.opType = opType;
        this.mailService = mailService;
        if(opType == MongoDBConfigConstants.UPD_TYPE){
            allKeys = new ArrayList<Subject>();
        }
    }
    @Override
    void execute(SqlRow row){
        logger.trace("数据入库开始");
        def start = System.currentTimeMillis()
        String esV =PropertiesUtil.getProperty("es");
        try{
            Subject obj = new Subject();
            Subject keys = new Subject();
            props.each {
                def val = row.get(it.prop_name);
                if(val!=null){
//                    if(it.prop_typ.equals("Decimal")){
////                        val = Double.valueOf(val)
//                    }else
                    if(val instanceof Clob){
                        val = StringUtils.ClobToString(val)
                    }
                    if(opType !=MongoDBConfigConstants.INS_TYPE && paramNames.contains(it.prop_name)){
                        keys.put(it.prop_name,val);
                    }
                    obj.put(it.prop_name,val)
                }
            }
            if(opType ==MongoDBConfigConstants.INS_TYPE ){
                if(null != esV && esV.equals("1")) {
//                    opEs(obj);
                }
//                subjectTemplate.save(obj,dsNm)
                subjectTemplate.save(dsNm,obj)
            } else if(opType ==MongoDBConfigConstants.UPD_TYPE){
                if(null != esV && esV.equals("1")) {
//                    opEs(obj);
                }
                if(!allKeys.contains(keys)){
                    allKeys.add(keys);
                    Map paramMap = [:]
                    keys.each {
                        paramMap.put(it.key,it.value)
                    }
//                    Criteria criatira = new Criteria();
//                    List<Criteria> list = new ArrayList<Criteria>();
//                    keys.each {
//                        list.add(Criteria.where(it.key).is(it.value));
//
//                    }
//                    Criteria[] criatiras = list.toArray();
//                    criatira.andOperator(criatiras);
//                    subjectTemplate.remove(new Query(criatira),dsNm);
                    subjectTemplate.deleteByParam(dsNm,paramMap)
//                    list = null;
                }
//                subjectTemplate.save(obj,dsNm)
                subjectTemplate.save(dsNm,obj)
            } else if(opType ==EsConfigConstants.UPD_TYPE){
//                opEs(obj);
            } else if(opType ==MongoDBConfigConstants.DEL_TYPE ){
                Map paramMap = [:]
                keys.each {
                    paramMap.put(it.key,it.value)
                }

//                Criteria criatira = new Criteria();
//                List<Criteria> list = new ArrayList<Criteria>();
//                keys.each {
//                    list.add(Criteria.where(it.key).is(it.value));
//                }
//                Criteria[] criatiras = list.toArray();
//                criatira.andOperator(criatiras);
//                subjectTemplate.remove(new Query(criatira),dsNm);
//                list = null;
                subjectTemplate.deleteByParam(dsNm,paramMap)
            }
        }catch (err){
            logger.error("=========错误：row:"+row+"--------dsNm:"+dsNm,err);
            boolean flg = MysqlSubjectServiceImplNew.saveErrMsg(dsNm,err.getMessage(),"data")
            if(flg){
                mailService.sendMail("专题数据源【"+dsNm + "】数据保存失败","专题数据源【"+dsNm+ "】数据保存失败，错误信息为："+err)
            }
        }
        def end = System.currentTimeMillis()
        logger.trace("数据入库完成，耗时："+(end-start))
    }

}