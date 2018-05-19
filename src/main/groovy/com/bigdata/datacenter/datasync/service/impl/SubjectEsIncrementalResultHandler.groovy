package com.bigdata.datacenter.datasync.service.impl

import com.alibaba.fastjson.JSON;
import com.avaje.ebean.SqlRow
import com.bigdata.datacenter.datasync.utils.constants.EsConfigConstants
import com.bigdata.datacenter.datasync.utils.constants.MongoDBConfigConstants
import com.bigdata.datacenter.datasync.model.mongodb.Subject
import com.bigdata.datacenter.metadata.service.ResultHandler

import org.apache.log4j.Logger

import com.bigdata.datacenter.datasync.utils.ESearchHelper;
import com.bigdata.datacenter.datasync.model.es.Author;
import com.bigdata.datacenter.datasync.utils.PropertiesUtil;
import com.bigdata.datacenter.datasync.model.es.CbdNews;
import com.bigdata.datacenter.datasync.model.es.Industry;
import com.bigdata.datacenter.datasync.model.es.Security;
import com.bigdata.datacenter.datasync.model.es.Image;
import com.bigdata.datacenter.datasync.model.es.Attachment;

   
/**
 * es专题增量数据
 */

class SubjectEsIncrementalResultHandler implements ResultHandler{
    private static final Logger logger = Logger.getLogger(SubjectEsIncrementalResultHandler.class)

    def String opType;
 
    def int updCnt=0;
  
    SubjectEsIncrementalResultHandler(){}
     
    SubjectEsIncrementalResultHandler(String opType){
        this.opType = opType;
    }
    @Override  
    void execute(SqlRow row){
        logger.debug("数据入库开始");
		println ("-----insert record");
        def start = System.currentTimeMillis()
	
        try{
           /* props.each {
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
            }*/
			String esV =PropertiesUtil.getProperty("es");
            if(opType ==EsConfigConstants.INS_TYPE ){
				if(null != esV && esV.equals("1")) {
					opEs(row);
				}
            }else if(opType ==EsConfigConstants.UPD_TYPE){
			    if(null != esV && esV.equals("1")) {
				   opEs(row);
			    } else {
				   if(updCnt==0){
//					  subjectTemplate.remove(keys,dsNm);
					  updCnt++;
				    }
//				    subjectTemplate.save(obj,dsNm)
				}
                
            } else if(opType ==EsConfigConstants.DEL_TYPE ){
//                subjectTemplate.remove(keys,dsNm);
            }
        }catch (err){
            logger.error("=========错误：row:"+row,err);
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
		/*if(obj.SOURCE_CODE) {
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
