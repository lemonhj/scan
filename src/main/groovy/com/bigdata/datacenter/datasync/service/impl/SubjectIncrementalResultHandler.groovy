package com.bigdata.datacenter.datasync.service.impl

import com.alibaba.fastjson.JSON;
import com.avaje.ebean.SqlRow
import com.bigdata.datacenter.datasync.utils.constants.MongoDBConfigConstants
import com.bigdata.datacenter.datasync.utils.constants.EsConfigConstants
import com.bigdata.datacenter.datasync.model.mongodb.Subject
import com.bigdata.datacenter.metadata.service.ResultHandler
import com.bigdata.datacenter.datasync.utils.StringUtils

import org.apache.log4j.Logger
import org.springframework.data.mongodb.core.MongoOperations

import com.bigdata.datacenter.datasync.utils.ESearchHelper;
import com.bigdata.datacenter.datasync.model.es.Author;
import com.bigdata.datacenter.datasync.utils.PropertiesUtil;
import com.bigdata.datacenter.datasync.model.es.CbdNews;
import com.bigdata.datacenter.datasync.model.es.Industry;
import com.bigdata.datacenter.datasync.model.es.Security;
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import com.bigdata.datacenter.datasync.model.es.Image;
import com.bigdata.datacenter.datasync.model.es.Attachment;


import java.sql.Clob
   
/**
 * 专题增量数据
 * Created by qq on 2017/5/24.
 */
class SubjectIncrementalResultHandler implements ResultHandler{
    private static final Logger logger = Logger.getLogger(SubjectIncrementalResultHandler.class)
    def String dsNm;
    def MongoOperations subjectTemplate;
    def List<Map<String, Object>> props;
    def String opType;
    def List<String> paramNames;
	def MailService mailService;
	def  List<Subject> allKeys = null;

	SubjectIncrementalResultHandler(){}
   
    SubjectIncrementalResultHandler(MongoOperations subjectTemplate, List<Map<String, Object>> props,
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

	SubjectIncrementalResultHandler(MongoOperations subjectTemplate, List<Map<String, Object>> props,
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
        logger.debug("数据入库开始");
        def start = System.currentTimeMillis()
//		String esV =PropertiesUtil.getProperty("es");
        try{
            Subject obj = new Subject();
            Subject keys = new Subject();   
            props.each {
                def val = row.get(it.prop_name);
                if(val!=null){
                    if(it.prop_typ.equals("Decimal")){
						val = Double.valueOf(val)
					}else if(val instanceof Clob){
						val = StringUtils.ClobToString(val)
					}
                    if(opType !=MongoDBConfigConstants.INS_TYPE && paramNames.contains(it.prop_name)){
                        keys.put(it.prop_name,val);
                    }
                    obj.put(it.prop_name,val)
                }
            }
            if(opType ==MongoDBConfigConstants.INS_TYPE ){
//				if(null != esV && esV.equals("1")) {
//					opEs(obj);
//				}
				subjectTemplate.save(obj,dsNm)
			} else if(opType ==MongoDBConfigConstants.UPD_TYPE){
//			    if(null != esV && esV.equals("1")) {
//				   opEs(obj);
//			    }
				if(!allKeys.contains(keys)){
					allKeys.add(keys);
					Criteria criatira = new Criteria();
					List<Criteria> list = new ArrayList<Criteria>();
					keys.each {
						list.add(Criteria.where(it.key).is(it.value));
					}
					Criteria[] criatiras = list.toArray();
					criatira.andOperator(criatiras);
					subjectTemplate.remove(new Query(criatira),dsNm);
					list = null;
				}
				subjectTemplate.save(obj,dsNm)
            }
//			else if(opType ==EsConfigConstants.UPD_TYPE){
//				   opEs(obj);
//			}
			else if(opType ==MongoDBConfigConstants.DEL_TYPE ){
				Criteria criatira = new Criteria();
				List<Criteria> list = new ArrayList<Criteria>();
				keys.each {
					list.add(Criteria.where(it.key).is(it.value));
				}
				Criteria[] criatiras = list.toArray();
				criatira.andOperator(criatiras);
				subjectTemplate.remove(new Query(criatira),dsNm);
				list = null;
//                subjectTemplate.remove(keys,dsNm);
            }
        }catch (err){
//            logger.error("=========错误：row:"+row+"--------dsNm:"+dsNm,err);
			boolean flg = SubjectServiceImplNew.saveErrMsg(dsNm,err.getMessage(),"data")
			if(flg){
				mailService.sendMail("专题数据源【"+dsNm + "】数据保存失败","专题数据源【"+dsNm+ "】数据保存失败，错误信息为："+err)
			}
        }
        def end = System.currentTimeMillis()
        logger.debug("数据入库完成，耗时："+(end-start))
    }
	
 private void opEs(SqlRow obj) {
	try {
		java.text.NumberFormat nf = java.text.NumberFormat.getInstance();
		nf.setGroupingUsed(false);
		String orid = nf.format(obj.ORIG_ID);
		ESearchHelper es = new ESearchHelper();
		List<Author> autLst = new ArrayList<Author>();
		CbdNews news = new CbdNews();
		news.setOrig_id(orid);
		news.setContent(obj.TITLE);
		news.data_type=obj.DATA_TYPE;
		List<Author> authors = null;
		if(obj.AUTHORS) {
			authors = JSON.parseObject(obj.AUTHORS,List.class);
		}
		news.authors = authors;
		/*if(obj.FLD_VAL) {
			for(Long fld: obj.FLD_VAL) {
				Author a = new Author();
				a.author_code = fld;
				autLst.add(a);
			}
		}
		news.authors = autLst;*/
		/*if(obj.COM_ID) {
			news.source_code=String.valueOf(nf.format(obj.COM_ID));
		} */
		news.source_code = obj.SOURCE_CODE;
		news.source_name=obj.COM_NAME;
		news.title = obj.TITLE; 
		news.pub_date = obj.PUB_DATE;
		news.upd_time = obj.UPD_TIME;
		news.es_time = new Date();
		news.description = obj.TITLE;
		if(obj.CATEGORY) {
			news.category = String.valueOf(nf.format(obj.CATEGORY));
		}
	   /* if(obj.industries !=null && obj.industries.size() > 0 ) {
			List<Industry> indLst = new ArrayList<Industry>();
			for(Long inid : obj.INDU_ID) {
				Industry ind = new Industry();
				ind.industry_code = String.valueOf(inid);
				ind.industry_name = "";
				indLst.add(ind);
			} 
			news.industries = indLst;
		}*/
		List<Industry> industries = null;
		if(obj.INDUSTRIES) {
			industries = JSON.parseObject(obj.INDUSTRIES,List.class);
	    }
		news.industries = industries;
		List<Image> images = null;
		if(obj.IMAGES) {
			images = JSON.parseObject(obj.IMAGES,List.class);
		}
		news.images = images;
		List<File> files = null;
		if(obj.FILES) {
			files = JSON.parseObject(obj.FILES,List.class);
		}
		news.files = files;
		List<Security> securitys = null;
		if (obj.SECURITIES){
			securitys = JSON.parseObject(obj.SECURITIES,List.class);
		}
		news.securities = securitys;
		es.addDocNew(news,"cbdnews")
	 } catch (err) {
         logger.error("opSubjectEs==============",err)
     }
  }
	
}
