package com.bigdata.datacenter.datasync.service.impl

import com.avaje.ebean.SqlRow
import com.bigdata.datacenter.datasync.model.mongodb.Subject
import com.bigdata.datacenter.metadata.service.ResultHandler
import com.bigdata.datacenter.datasync.utils.StringUtils
import org.apache.log4j.Logger
import org.springframework.data.mongodb.core.MongoOperations

import java.sql.Clob

/**
 * 专题静态数据
 * Created by qq on 2017/5/24.
 */
class SubjectResultHandler implements ResultHandler{
    private static final Logger logger = Logger.getLogger(SubjectResultHandler.class)
    def String dsNm;
    def MongoOperations subjectTemplate;
    def List<Map<String, Object>> props;
    def Map<String,String> idxMap;
    def MailService mailService;

    SubjectResultHandler(){}

    SubjectResultHandler(MongoOperations subjectTemplate,List<Map<String, Object>> props,String dsNm){
        this.subjectTemplate = subjectTemplate;
        this.props = props;
        this.dsNm = dsNm;
    }

    SubjectResultHandler(MongoOperations subjectTemplate,List<Map<String, Object>> props,String dsNm,Map<String,String> idxMap){
        this.subjectTemplate = subjectTemplate;
        this.props = props;
        this.dsNm = dsNm;
        this.idxMap = idxMap;
    }

    SubjectResultHandler(MongoOperations subjectTemplate,List<Map<String, Object>> props,String dsNm,MailService mailService){
        this.subjectTemplate = subjectTemplate;
        this.props = props;
        this.dsNm = dsNm;
        this.mailService = mailService;
    }

    SubjectResultHandler(MongoOperations subjectTemplate,List<Map<String, Object>> props,String dsNm,Map<String,String> idxMap,MailService mailService){
        this.subjectTemplate = subjectTemplate;
        this.props = props;
        this.dsNm = dsNm;
        this.idxMap = idxMap;
        this.mailService = mailService;
    }
    @Override
    void execute(SqlRow row){
        logger.debug("数据入库开始");
        def start = System.currentTimeMillis()
        try{
            Subject obj = new Subject();
            props.each {
                def val = row.get(it.prop_name);
                if(!it.prop_name.equals("COM_CN_SEARCH") && val!=null){
                    if(it.prop_typ.equals("Decimal")){
                        val = Double.valueOf(row.get(it.prop_name))
                    }else if(val instanceof Clob){
                        val = StringUtils.ClobToString(val)
                    }
                    obj.put(it.prop_name,val)
                }
            }
            if(idxMap!=null){
                obj.put(idxMap.get("idxName"),idxMap.get("idxVal"));
            }
            subjectTemplate.save(obj,dsNm)
        }catch (err){
//            logger.error("========数据源["+dsNm+"]保存数据源错误，row:"+row+"-------error:"+err)
            boolean flg = SubjectServiceImplNew.saveErrMsg(dsNm,err.getMessage(),"data")
            if(flg){
                mailService.sendMail("专题数据源【"+dsNm + "】数据保存失败","专题数据源【"+dsNm+ "】数据保存失败，错误信息为："+err)
            }
        }
        def end = System.currentTimeMillis()
        logger.debug("数据入库完成，耗时："+(end-start))
    }
}
