package com.bigdata.datacenter.datasync.service.impl.mysql

import com.avaje.ebean.SqlRow
import com.bigdata.datacenter.datasync.service.impl.MailService
import com.bigdata.datacenter.datasync.utils.constants.MongoDBConfigConstants
import com.bigdata.datacenter.metadata.service.ResultHandler
import org.apache.log4j.Logger

/**
 * Created by Dan on 2017/7/12.
 */
class AllIndexMacroDsResultHandler implements ResultHandler{
    private static final Logger logger = Logger.getLogger(AllIndexMacroDsResultHandler.class)
    private DBCommon_mysql dbCommon_mysql
    private List props
    private String dsNm
    public Date maxUpdTime
    private static final String MACRO_IDX_ID = "IDX_ID";
	private static final String MACRO_END_DT = "ENDDATE";
	private static final String MACRO_IDX_VAL = "IDX_VAL";
	private static final String MACRO_UPD_TIME = "UPD_TIME";
    def MailService mailService;

    public AllIndexMacroDsResultHandler(DBCommon_mysql dbCommon_mysql, List props,
                                        Date maxUpdTime, String dsNm){

        this.dbCommon_mysql = dbCommon_mysql
        this.props = props
        this.dsNm = dsNm
        this.maxUpdTime = maxUpdTime
    }

    public AllIndexMacroDsResultHandler(DBCommon_mysql dbCommon_mysql, List props,
                                        Date maxUpdTime, String dsNm,MailService mailService){

        this.dbCommon_mysql = dbCommon_mysql
        this.props = props
        this.dsNm = dsNm
        this.maxUpdTime = maxUpdTime
        this.mailService = mailService
    }

    @Override
    void execute(SqlRow row) {
        Map obj = [:]
        Map key = [:]
        for (int k = 0; k < props.size(); k++) {
            if (props.get(k).get("prop_name").equals(MACRO_IDX_ID)
                    || props.get(k).get("prop_name").equals(MACRO_IDX_VAL)
                    || props.get(k).get("prop_name").equals(MACRO_END_DT)
                    || props.get(k).get("prop_name").equals(MACRO_UPD_TIME)) {

                setPro(obj, props.get(k), row)
            }
            if (props.get(k).get("prop_name").equals(MACRO_IDX_ID)
                    || props.get(k).get("prop_name").equals(MACRO_END_DT)) {

                setPro(key, props.get(k), row)
            }
        }

        try{
            dbCommon_mysql.saveOrUpdate(MongoDBConfigConstants.IDX_MARCO,obj,["IDX_ID":key.get(MACRO_IDX_ID)]);
            Date updTime = (Date) obj.get(MACRO_UPD_TIME)
            if (updTime.compareTo(maxUpdTime) > 0) {
                maxUpdTime = updTime
                // 保存最新更新时间
                saveMacroMaxUpdTime(maxUpdTime, dsNm)
            }
        }catch (err){
            logger.error("宏观指标【"+key.get(MACRO_IDX_ID)+"】保存错误，========="+obj)
            mailService.sendMail("宏观指标【"+key.get(MACRO_IDX_ID) + "】数据保存失败","宏观指标【"+key.get(MACRO_IDX_ID)+ "】数据保存失败，错误信息为："+err+",错误数据为："+obj)
        }

    }

    private void saveMacroMaxUpdTime(Date maxUpdTime, String dsNm) {
		boolean existFlg = false
        def list =  dbCommon_mysql.find(MongoDBConfigConstants.IDX_MARCO_UPD,["DS_NAME":dsNm]);
        if(list != null && list.size()>0){
            Map old = (Map)list.get(0);
            if (old != null) {
                Date oldUpdTime = (Date)old.get("MAX_UPD_TIME");
                // MAX_UPD_TIME小于最新时间则更新
                if (oldUpdTime.compareTo(maxUpdTime) < 0) {
                    Map obj = [:]
                    obj.put("DS_NAME",dsNm)
                    obj.put("MAX_UPD_TIME", maxUpdTime)
                    dbCommon_mysql.update(MongoDBConfigConstants.IDX_MARCO_UPD,obj,["DS_NAME":dsNm]);
                }
                existFlg = true
            }
        }
		if (!existFlg) {
			Map obj = [:]
			obj.put("DS_NAME",dsNm)
			obj.put("MAX_UPD_TIME", maxUpdTime)
            dbCommon_mysql.insert(MongoDBConfigConstants.IDX_MARCO_UPD,obj);
		}
	}

    private void setPro(Map obj, Map prop, SqlRow row) {
        if (prop.get("prop_typ") == "Decimal") {
            Double val = row.getDouble(prop.get("prop_name"))
            if (val == null) {
                obj.put(prop.get("prop_name"), null)
            } else {
                obj.put(prop.get("prop_name"), val)
            }
        }
        if (prop.get("prop_typ") == "String") {
            String val = row.getString(prop.get("prop_name"))
            if (val.isEmpty()) {
                obj.put(prop.get("prop_name"), null)
            } else {
                obj.put(prop.get("prop_name"), val)
            }
        }
        if (prop.get("prop_typ") == "DateTime") {
            def dateTime = row.getTimestamp(prop.get("prop_name"))
            if (dateTime == null) {
                obj.put(prop.get("prop_name"), null)
            } else {
                def val = new Date(dateTime.getTime())
                obj.put(prop.get("prop_name"), val)
            }
        }
    }
}
