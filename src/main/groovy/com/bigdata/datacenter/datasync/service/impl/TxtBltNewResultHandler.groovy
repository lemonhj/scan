package com.bigdata.datacenter.datasync.service.impl

import java.util.Map;

import com.avaje.ebean.SqlRow
import com.bigdata.datacenter.datasync.model.mongodb.BltNew
import com.bigdata.datacenter.metadata.service.ResultHandler
import com.bigdata.datacenter.datasync.utils.EntityUtil

import org.apache.log4j.Logger
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update

import com.bigdata.datacenter.datasync.utils.ESearchHelper;
import com.bigdata.datacenter.datasync.model.es.Author;
import com.bigdata.datacenter.datasync.utils.PropertiesUtil;
import com.bigdata.datacenter.datasync.model.es.CbdNews;
import com.bigdata.datacenter.datasync.model.es.Industry;
import com.bigdata.datacenter.datasync.model.es.Security;
import java.text.*;
import java.sql.Timestamp;
import com.bigdata.datacenter.datasync.model.es.Attachment;

/**
 * Created by Dan on 2017/6/7.
 */
class TxtBltNewResultHandler implements ResultHandler{
     private static final Logger logger = Logger.getLogger(TxtBltNewResultHandler.class)

    private MongoOperations txtBltTemplate
    List<Map<String, Object>> props
    Map secuidMap = [:]
    String dsName
    def id
    private List blts = []
    def MailService mailService
         
    public List getBlts() {
        return blts
    }

    public TxtBltNewResultHandler(){}

    public TxtBltNewResultHandler (String dsName, MongoOperations txtBltTemplate, List<Map<String, Object>> props, Map secuidMap) {
        this.props = props
        this.dsName = dsName
        this.secuidMap = secuidMap
        this.txtBltTemplate = txtBltTemplate
    }

    public TxtBltNewResultHandler (String dsName, MongoOperations txtBltTemplate, List<Map<String, Object>> props, Map secuidMap,MailService mailService) {
        this.props = props
        this.dsName = dsName
        this.secuidMap = secuidMap
        this.txtBltTemplate = txtBltTemplate
        this.mailService = mailService;
    }

    @Override
    void execute(SqlRow row) {
        logger.debug("数据读取开始")
        def start = System.currentTimeMillis()

        this.setBltValue(row)
        def end = System.currentTimeMillis()
        logger.debug("数据入口结束，耗时："+(end-start))
    }

    def setBltValue(SqlRow row){
        Map obj = [:]
        for (int i = 0; i<props.size(); i++){
            def item = props[i]
            if (item.get("prop_typ") == "Decimal") {
                if (row.get(item.get("prop_name")) == null ) {
                    obj.put(item.get("prop_name"), null)
                    continue
                }
                def val = row.getDouble(item.get("prop_name"))
                obj.put(item.get("prop_name"), val)
                if (item.get("prop_name") == "ID" ) {
                    if (id == null) {
                        id = val
                    }
                    if (id.doubleValue() != val.doubleValue()) {
                        saveBlt(blts)
                        id = val
                        blts = [];
                    }
                }
            }
            if (item.prop_typ == "String") {
                def val = row.get(item.get("prop_name"))
                if (val == null || val == "") {
                    obj.put(item.get("prop_name"), null)
                    continue
                }else{
                    obj.put(item.get("prop_name"), val)
                }
            }
            if (item.get("prop_typ") == "DateTime") {
                if (row.get(item.get("prop_name")) == null) {
                    obj.put(item.get("prop_name"), null)
                    continue
                }
                def dateTime = row.getTimestamp(item.get("prop_name"))
                def val = new Date(dateTime.getTime())
                obj.put(item.get("prop_name"), val)
            }
        }
        blts.add(obj)
    }

    def saveBlt(List blts){
        try{
            Map result = [:]
            HashSet secuIdLst = new HashSet()
            for (blt in blts){
                if (blt.get("SECU_ID") != null) {
                    secuIdLst.add((Integer)blt.get("SECU_ID"))
                }
                if (blt.get("SUB_TYP_CODE") != null) {
                    int subTypCd = Integer.parseInt(blt.get("SUB_TYP_CODE"))
                    if ((subTypCd == 10 || subTypCd == 20) && blt.get("TYP_CODEI") != null && blt.get("TYP_CODEI") == 10) {
                        result = blt;
                    } else if (subTypCd == 11) {
                        String trdCd = blt.get("TRD_CODE").toString()
                        if (trdCd != null && trdCd.toUpperCase().contains("M")) {
                            result = blt
                            result.put("TRD_CODE", trdCd.substring(0,6))
                        }
                    } else if (subTypCd == 12) {
                        if (blt.get("TYP_CODEI") != null && blt.get("TYP_CODEI") == 12) {
                            result = blt
                        }
                    }
                }
            }
            if (blts.size() != 0) {
                if (result == null || result.size() == 0) {
                    result = blts.get(0)
                }
                def id = (Double) result.get("ID")
                def rlt = result
                def bltTmp = txtBltTemplate.findOne(new Query(Criteria.where("ID").is(id)), BltNew.class, dsName)
                if (secuIdLst.isEmpty()) {
                    rlt.put("SECU_ID", null)
                } else {
                    if (result.get("COM_ID") != null && result.get("SUB_TYP_CODE") != null) {
                        int subTypCd = Integer.parseInt(result.get("SUB_TYP_CODE"))
                        int comId = result.get("COM_ID")
                        if ((subTypCd == 10 || comId == 20) && secuidMap.get(comId) != null) {
                            secuIdLst.addAll(secuidMap.get(comId))
                            rlt.put("SECU_ID", secuIdLst)
                        } else {
                            rlt.put("SECU_ID", secuIdLst)
                        }
                    } else {
                        rlt.put("SECU_ID", null)
                    }
                }
                if (bltTmp == null) {
                    String esV =PropertiesUtil.getProperty("es");
                    if(null != esV && esV.equals("1")) {
                        java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
                        nf.setGroupingUsed(false);
                        String orid = rlt.get("ID");
                        ESearchHelper es = new ESearchHelper();
                        CbdNews news = new CbdNews();
                        if(null != orid) {
                            news.setOrig_id(nf.format(rlt.get("ID")));
                        }
                        news.setContent(rlt.get("TIT"));
                        news.data_type="2";
                        news.source_code=nf.format(rlt.get("COM_ID")==null?0:rlt.get("COM_ID"));
                        news.source_name=rlt.get("INFO_SOUR");
                        news.title=rlt.get("TIT");
                        news.pub_date=rlt.get("PUB_DT");
                        news.es_time = new Date();
                        news.upd_time =  rlt.get("UPD_TIME"); //new Timestamp(System.currentTimeMillis());
                        news.description = rlt.get("TIT");
                        news.com_id = nf.format(rlt.get("COM_ID")==null?0:rlt.get("COM_ID"));
                        ////news.typ_codei = nf.format(rlt.get("TYP_CODEI"));
                        news.category = nf.format(rlt.get("TYP_CODE") == null?0:rlt.get("TYP_CODE"));
                        if(rlt.get("SECU_ID") !=null && ((Set)rlt.get("SECU_ID")).size() > 0 ) {
                            List<Security> secLst = new ArrayList<Security>();
                            for(Long seid : rlt.get("SECU_ID")) {
                                Security sec = new Security();
                                sec.secu_id = String.valueOf(seid);
                                sec.secu_type = nf.format(rlt.get("TYP_CODEI")==null?0:rlt.get("TYP_CODEI"));
                                sec.secu_sht = rlt.get("SECU_SHT");
                                secLst.add(sec);
                            }
                            news.securities = secLst;
                        }
						List<Attachment> files = new ArrayList<Attachment>();
						Attachment att = new Attachment();
						if(rlt.get("ANN_ID") && rlt.get("ANN_FMT")) {
							att.fileurl = nf.format(rlt.get("ANN_ID"))+"."+rlt.get("ANN_FMT");
							att.filename = att.fileurl;
							files.add(att);
						}
		                news.files = files;
                        es.addDocNew(news,"cbdnews");
                    }
                     txtBltTemplate.save(rlt, dsName)
                } else {
                    //rlt.put("_id", bltTmp.get("_id"))
                    rlt.put("idx", bltTmp.getIdx())
                    Update up = EntityUtil.objectToUpdate(rlt)
                    txtBltTemplate.updateFirst(new Query(Criteria.where("ID").is(rlt.get(id))), up, dsName)
                }
            }
        }catch (err){
            logger.error("公告信息数据保存失败: "+err+",数据为：" + blts.get(0) )
            if(mailService!=null){
                mailService.sendMail("公告信息保存数据失败","【公告信息】保存数据失败,错误信息："+err);
            }
        }
    }

    def mapToObjectTo(Map map, Class clazz) {
        def instance = clazz.newInstance()

        map.each { key, value ->
            key = key.toUpperCase()
            instance[key] = value
        }
        return instance
    }
}

