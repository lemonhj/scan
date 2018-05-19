package com.bigdata.datacenter.datasync.service.impl

import com.avaje.ebean.SqlRow
import com.bigdata.datacenter.datasync.model.mongodb.Law
import com.bigdata.datacenter.datasync.model.mongodb.LawResult
import com.bigdata.datacenter.datasync.model.mongodb.Nws
import com.bigdata.datacenter.metadata.service.ResultHandler
import com.bigdata.datacenter.datasync.utils.EntityUtil
import com.bigdata.datacenter.datasync.utils.StringUtils
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
import java.sql.Timestamp;

import java.sql.Clob

/**
 * 提示信息数据回调并保存到mongo数据库
 * Created by qq on 2017/5/24.
 */
class TxtNwsResultHandler implements ResultHandler{
    private static final Logger logger = Logger.getLogger(TxtNwsResultHandler.class)
    def String dsName;
    def MongoOperations txtNwsTemplate;
    def List<Map<String, Object>> props;
    def String nwstyp;
    def MailService mailService

    TxtNwsResultHandler(){}

    TxtNwsResultHandler(MongoOperations txtNwsTemplate, String dsName,List<Map<String, Object>> props,String nwstyp){
        this.txtNwsTemplate = txtNwsTemplate;
        this.dsName = dsName;
        this.props = props;
        this.nwstyp = nwstyp;
    }

    TxtNwsResultHandler(MongoOperations txtNwsTemplate, String dsName,List<Map<String, Object>> props,String nwstyp,MailService mailService){
        this.txtNwsTemplate = txtNwsTemplate;
        this.dsName = dsName;
        this.props = props;
        this.nwstyp = nwstyp;
        this.mailService = mailService;
    }

    @Override
    void execute(SqlRow row){
        logger.debug("新闻数据读取开始");
        def start = System.currentTimeMillis()
        try{
            Nws nws = setValue(row)
            def result = txtNwsTemplate.findOne(new Query(Criteria.where("ID").is(nws.ID)),Nws.class,dsName);
            String esV =PropertiesUtil.getProperty("es");
            if(null != esV && esV.equals("1")) {
                java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
                nf.setGroupingUsed(false);
                String orid = nf.format(nws.ID);
                ESearchHelper es = new ESearchHelper();
                CbdNews news = new CbdNews();
                news.setOrig_id(orid);
                news.setContent(nws.CONT);
                news.data_type=3;
                if(nws.MED_CODE !=null){
                    news.source_code=nf.format(nws.MED_CODE);
                }
                news.source_name=nws.MED_NAME;
                news.title=nws.TIT;
                news.pub_date=nws.PUB_DT;
                news.upd_time =  nws.UPD_TIME;// new Timestamp(System.currentTimeMillis()); //
                news.es_time = new Date();
                if(nws.CONT!=null && nws.CONT.length()>300) {
                    news.setDescription(nws.CONT.substring(0,300)+"...");
                } else {
                    news.setDescription(nws.CONT);
                }
                if(nws.TYP_CODE !=null){
                    news.category = nf.format(nws.TYP_CODE[0]);
                }
                if(nws.INDU_ID !=null && nws.INDU_ID.size() > 0 ) {
                    List<Industry> indLst = new ArrayList<Industry>();
                    for(Long inid : nws.INDU_ID) {
                        Industry ind = new Industry();
                        ind.industry_code = String.valueOf(inid);
                        ind.industry_name = "";
                        indLst.add(ind);
                    }
                    news.industries = indLst;
                }
                if(nws.SECU_ID !=null) {
                    List<Security> secLst = new ArrayList<Security>();
                    Security sec = new Security();
                    if(nws.SECU_ID !=null){
                        sec.secu_id = nf.format(nws.SECU_ID);
                    }
                    sec.bd_code = nws.BD_CODE;
                    sec.secu_sht = nws.SECU_SHT;
                    secLst.add(sec);
                    news.securities = secLst;
                }
                String aut = nws.AUT;
                if(null != aut) {
                    String[] auts = aut.split("\\\\");
                    List<Author> autLst = new ArrayList<Author>();
                    for(int i=0; i<auts.length; i++) {
                        Author a = new Author();
                        a.author_code = "";
                        a.author_name = auts[i];
                        autLst.add(a);
                    }
                    news.authors = autLst;
                }
                es.addDocNew(news,"cbdnews");
            }
           if (result == null) {
              txtNwsTemplate.save(nws,dsName)
           } else {
              nws.put("idx",result._id);
              Update up = EntityUtil.objectToUpdate(nws)
              txtNwsTemplate.updateFirst(new Query(Criteria.where("ID").is(nws.ID)),up,dsName)
           }
        }catch (err){
            logger.error("新闻服务数据保存失败: "+err+",数据为：" + row )
            if(mailService!=null){
                mailService.sendMail("新闻服务保存数据失败","【新闻服务】保存数据失败,错误信息："+err);
            }
        }
       def end = System.currentTimeMillis()
       logger.debug("新闻数据入口结束，耗时："+(end-start))
    }

    /**
     * 设置值
     * @param row
     * @return
     */
    Nws setValue(SqlRow row){
        Nws nws = new Nws();
        props.each {
            def val = row.get(it.prop_name);
            if(val!=null&&it.prop_typ.equals("Decimal")){
                val = Double.valueOf(row.get(it.prop_name))
            }
            nws.put(it.prop_name,val)
        }
        String nwsRsId = (String) row.get("RS_ID");
        String typCode = row.get("TYP_CODE");
		
        // NWS or NC
        String nwsTyp = (String) row.get("NWS_TYP");
        List<HashMap<String, Object>> stocks = new ArrayList<HashMap<String,Object>>();
        List<HashMap<String, Object>> marks = new ArrayList<HashMap<String,Object>>();
        List<Double> SECT_ID = new ArrayList<Double>();
        List<Double> INDU_ID = new ArrayList<Double>();
        List<Double> TYP_CODE = new ArrayList<Double>();
        if (row.get("TRD_CODE") != null && row.get("SECU_ID") != null && row.get("SECU_SHT") != null) {
            HashMap<String, Object> stock = new HashMap<String, Object>();
            stock.put("TRD_CODE", row.get("TRD_CODE"));
            stock.put("SECU_ID",row.get("SECU_ID"));
            stock.put("SECU_SHT", row.get("SECU_SHT"));
            stocks.add(stock);
            if (nwsTyp != null && nwsRsId != null
               && nwsTyp.equals("NWS") && nwsRsId.equals("CBN")) {
                // 标签数据MARK字段
                HashMap<String, Object> mark = new HashMap<String, Object>();
                mark.put("ID", row.get("SECU_ID"));
                mark.put("NAME", row.get("SECU_SHT"));
                mark.put("TYPE", 2);
                mark.put("EVT_DIR", row.get("SECU_EVT_DIR"));
                mark.put("EVT_ST", row.get("SECU_EVT_ST"));
                mark.put("BD_CODE", row.get("BD_CODE"));
                marks.add(mark);
            }
        }
        if (TYP_CODE !=null && row.get("TYP_CODE") != null && !TYP_CODE.contains(row.get("TYP_CODE"))) {
            TYP_CODE.add((Double) row.get("TYP_CODE"));
            if (nwsTyp != null && nwsRsId != null) {
                if (nwsTyp.equals("NWS") && nwsRsId.equals("CBN")) {
                    // 标签数据MARK字段
                    if (typCode.length() > 2 && typCode.substring(0,2).equals("96")) {
                        HashMap<String, Object> mark = new HashMap<String, Object>();
                        mark.put("ID", row.get("TYP_CODE"));
                        mark.put("NAME", row.get("CST_DESC"));
                        mark.put("TYPE", 1);
                        mark.put("EVT_DIR", 0);
                        mark.put("EVT_ST", 0);
                        mark.put("BD_CODE", null);
                        marks.add(mark);
                    }
                }
            }
        }
        if (SECT_ID!=null && row.get("SECT_ID") != null && !SECT_ID.contains(row.get("SECT_ID"))) {
            SECT_ID.add((Double) row.get("SECT_ID"));
            if (nwsTyp != null && nwsRsId != null) {
                if (nwsTyp.equals("NWS") && nwsRsId.equals("CBN")) {
                    // 标签数据MARK字段
                    HashMap<String, Object> mark = new HashMap<String, Object>();
                    mark.put("ID", row.get("SECT_ID"));
                    mark.put("NAME", row.get("SECT_NAME"));
                    mark.put("TYPE", 3);
                    mark.put("EVT_DIR", row.get("SECT_EVT_DIR"));
                    mark.put("EVT_ST", row.get("SECT_EVT_ST"));
                    mark.put("BD_CODE", null);
                    marks.add(mark);
                }
            }
        }
        if (INDU_ID!=null && row.get("INDU_ID") != null && !INDU_ID.contains(row.get("INDU_ID"))) {
            INDU_ID.add((Double) row.get("INDU_ID"));
        }
        nws.put("TYP_CODE", TYP_CODE);
        nws.put("SECT_ID", SECT_ID);
        nws.put("INDU_ID", INDU_ID);
        nws.put("STOCKS", stocks);
        if (nwstyp.equals("NWS")) {
            nws.put("MARKS", marks);
        }
        return removeNullVal(nws)
    }

    /**
     * 去除为空的数据
     * @param nws
     */
    Nws removeNullVal(Nws nws) {
        Nws ret = new Nws();
        nws.each {
            if(it.value!=null){
                if(it.value instanceof  List){
                    List list = (List)it.value
                    if(list.size()>0){
                        ret.put(it.key,list)
                    }
                }else if(it.value instanceof Clob){
                    ret.put(it.key,StringUtils.ClobToString(it.value))
                }else{
                    ret.put(it.key,it.value)
                }
            }
        }
        return ret
    }
}
