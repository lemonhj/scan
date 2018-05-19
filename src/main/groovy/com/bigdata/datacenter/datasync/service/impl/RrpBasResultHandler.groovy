package com.bigdata.datacenter.datasync.service.impl

import java.sql.Timestamp;

import com.avaje.ebean.SqlRow
import com.bigdata.datacenter.datasync.service.XmlRawSQLService
import com.bigdata.datacenter.datasync.utils.DateUtils
import com.bigdata.datacenter.datasync.model.es.CbdNews;
import com.bigdata.datacenter.datasync.model.es.Industry;
import com.bigdata.datacenter.datasync.model.es.Security;
import com.bigdata.datacenter.datasync.model.mongodb.ReportBas
import com.bigdata.datacenter.metadata.service.RawSQLService
import com.bigdata.datacenter.metadata.service.ResultHandler
import com.bigdata.datacenter.datasync.utils.EntityUtil

import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update

import com.bigdata.datacenter.datasync.utils.ESearchHelper;
import com.bigdata.datacenter.datasync.model.es.Author;
import com.bigdata.datacenter.datasync.utils.PropertiesUtil;

/**
 * 研报数据回调并保存到mongo数据库
 * Created by qq on 2017/5/24.
 */

class RrpBasResultHandler implements ResultHandler{
    private static final Logger logger = Logger.getLogger(RrpBasResultHandler.class)
    Class clazz;
    MongoOperations rrpTemplate
//    RawSQLService rawSQLService
    XmlRawSQLService xmlRawSQLService
   
    RrpBasResultHandler(){}

    RrpBasResultHandler(MongoOperations rrpTemplate, Class clazz, XmlRawSQLService xmlRawSQLService){
        this.rrpTemplate = rrpTemplate;
        this.clazz = clazz;
        this.xmlRawSQLService = xmlRawSQLService
    }
 
	
	
    @Override
    void execute(SqlRow row){
        logger.debug("研报数据读取开始");
        def start = System.currentTimeMillis()
        try {
            def rrpBas = EntityUtil.mapToObject(row, clazz)
            rrpBas = getRelateData(rrpBas)
            def result = rrpTemplate.findOne(new Query(Criteria.where("ID").is(rrpBas.ID)), clazz)
            String esV = PropertiesUtil.getProperty("es");
            if (null != esV && esV.equals("1")) {
                java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
                nf.setGroupingUsed(false);
                String orid = nf.format(rrpBas.ID);
                ESearchHelper es = new ESearchHelper();
                List<Author> autLst = new ArrayList<Author>();
                CbdNews news = new CbdNews();
                news.setOrig_id(orid);
                news.setContent(rrpBas.ABST_SHT);
                news.data_type = "1";
                if (rrpBas.FLD_VAL) {
                    for (Long fld : rrpBas.FLD_VAL) {
                        Author a = new Author();
                        a.author_code = fld;
                        autLst.add(a);
                    }
                }
                if(rrpBas.AUT) {
                    String[] auts = rrpBas.AUT.split("\\\\");
                    if(auts != null && auts.length >0 ) {
                        try
                        {
                         if(autLst != null && autLst.size() > 0) {
                            for(int i = 0; i< autLst.size(); i++) {
                              autLst.get(i).setAuthor_name(auts[i]);
                            }
                         }
                        } catch(Exception e) {
                           e.printStackTrace();
                        }
                    }
                }
                news.authors = autLst;
                if(rrpBas.COM_ID !=null){
                    news.source_code=String.valueOf(nf.format(rrpBas.COM_ID));
                }
                news.source_name=rrpBas.COM_NAME;
                news.title = rrpBas.TIT;
                news.pub_date = rrpBas.PUB_DT;
                news.upd_time = rrpBas.UPD_TIME;  // new Timestamp(System.currentTimeMillis());
                news.es_time = new Date();
                news.description = rrpBas.ABST_SHT;
                news.category = nf.format(rrpBas.RPT_TYP_CODE);
                if(rrpBas.getINDU_ID() !=null && rrpBas.getINDU_ID().size() > 0 ) {
                    List<Industry> indLst = new ArrayList<Industry>();
                    for(Long inid : rrpBas.getINDU_ID()) {
                        Industry ind = new Industry();
                        ind.industry_code = String.valueOf(inid);
                        ind.industry_name = "";
                        indLst.add(ind);
                    }
                    news.industries = indLst;
                }
                if(rrpBas.getSECU_ID() !=null && rrpBas.getSECU_ID().size() > 0 ) {
                    List<Security> secLst = new ArrayList<Security>();
                    for(Long seid : rrpBas.getSECU_ID()) {
                        Security sec = new Security();
                        sec.secu_id = String.valueOf(seid);
                        secLst.add(sec);
                    }
                    news.securities = secLst;
                }
                es.addDocNew(news,"cbdnews");
            }
           if (result == null) {
              rrpTemplate.save(rrpBas);
           } else {
              rrpBas.setIdx(result.idx);
              Update up = EntityUtil.objectToUpdate(rrpBas)
              rrpTemplate.updateFirst(new Query(Criteria.where("ID").is(result.ID)),up,clazz)
           }
        }catch (err){
            logger.error("研报数据保存失败，"+err+" .错误数据为："+row)
        }

        def end = System.currentTimeMillis()
        logger.debug("研报数据入口结束，耗时："+(end-start))
    }   

    // 获取相关数据
      ReportBas getRelateData(ReportBas result) {
        List<Long> induIds =  mapToLong(xmlRawSQLService.queryRawSqlByKey("selectInduIdsByOrgId",[origId:result.ID]));
        List<Long> secuIds = mapToLong(xmlRawSQLService.queryRawSqlByKey("selectSecuIdByOrgId",[origId:result.ID]));
        List<Long> fldVals = mapToLong(xmlRawSQLService.queryRawSqlByKey("selectFldvalsByOrgId",[origId:result.ID]));
        List<Map<String,Object>> induRatLst = xmlRawSQLService.queryRawSqlByKey("selectInduRatByOrgId",[origId:result.ID]);
        List<Map<String,Object>> secuRatLst = xmlRawSQLService.queryRawSqlByKey("selectSecuRatByOrgId",[origId:result.ID]);
        // 由于一份研报可能对应多个行业评级或者多个股票评级，做出如下要求：
        // 如果关联到一行记录，则写入关联数据，如果关联大于1行或者没有关联，则字段对应的值写入 null .
        // 为了保持对象保存到mongo时的字段顺序
        result.setINDU_RAT_ORIG_DESC(null);
        result.setINDU_RAT_ORIG_DESC_LST(null);
        if (induRatLst != null && induRatLst.size() == 1) {
            if (induRatLst.get(0) != null) {
                result.setINDU_RAT_ORIG_DESC((String)induRatLst.get(0).get("INDU_RAT_ORIG_DESC"));
                result.setINDU_RAT_ORIG_DESC_LST((String)induRatLst.get(0).get("INDU_RAT_ORIG_DESC_LST"));
            }
        }
        // 为了保持对象保存到mongo时的字段顺序
        result.setRAT_ORIG_DESC(null);
        result.setRAT_ORIG_DESC_LST(null);
        result.setTARG_PRC_MIN(null);
        result.setTARG_PRC_MAX(null);
        if (secuRatLst != null && secuRatLst.size() == 1) {
            if (secuRatLst.get(0) != null) {
                result.setRAT_ORIG_DESC((String)secuRatLst.get(0).get("RAT_ORIG_DESC"));
                result.setRAT_ORIG_DESC_LST((String)secuRatLst.get(0).get("RAT_ORIG_DESC_LST"));
                if (secuRatLst.get(0).get("TARG_PRC_MIN") != null) {
                    result.setTARG_PRC_MIN(((BigDecimal)secuRatLst.get(0).get("TARG_PRC_MIN")).doubleValue());
                } else {
                    // 为了保持对象保存到mongo时的字段顺序
                    result.setTARG_PRC_MIN(null);
                }
                if (secuRatLst.get(0).get("TARG_PRC_MAX") != null) {
                    result.setTARG_PRC_MAX(((BigDecimal)secuRatLst.get(0).get("TARG_PRC_MAX")).doubleValue());
                } else {
                    // 为了保持对象保存到mongo时的字段顺序
                    result.setTARG_PRC_MAX(null);
                }
            }
        }
        result.setINDU_ID(induIds);
        result.setSECU_ID(secuIds);
        result.setFLD_VAL(fldVals);

        Date pub_date =DateUtils.string2Date(DateUtils.date2String(result.getPUB_DT(), DateUtils.FORMAT_DATE), DateUtils.FORMAT_DATE);
        Date ent_time = DateUtils.string2Date(DateUtils.date2String(result.getENT_TIME(), DateUtils.FORMAT_DATE), DateUtils.FORMAT_DATE);
        Calendar pubDateCal = Calendar.getInstance();
        pubDateCal.setTime(pub_date);
        Calendar entTimeCal = Calendar.getInstance();
        entTimeCal.setTime(ent_time);
        // 当 ENT_TIME(入库时间) 字段的日期部分大于 PUB_DT(发布时间) 对应的日期时，将ENT_TIME 的日期部分改为PUB_DT 对应的日期值。
        if (pub_date.compareTo(ent_time) < 0) {
           entTimeCal.setTime(result.getENT_TIME());
           entTimeCal.set(pubDateCal.get(Calendar.YEAR), pubDateCal.get(Calendar.MONTH), pubDateCal.get(Calendar.DAY_OF_MONTH), entTimeCal.get(Calendar.HOUR), entTimeCal.get(Calendar.MINUTE), entTimeCal.get(Calendar.SECOND));
           result.setENT_TIME(entTimeCal.getTime());
        }
        return result;
    }

    List<Long> mapToLong(List<Map<String,Object>> result){
        if(result!=null&&result.size()>0){
            List<Long> list = new ArrayList<Long>()
            for (Map<String,Object> map : result){
                list.add((Long)(map.values()).getAt(0))
            }
            return list
        }else{
            return null
        }
    }
}
