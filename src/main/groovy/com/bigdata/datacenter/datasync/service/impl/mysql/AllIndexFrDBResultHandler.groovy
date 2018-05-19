package com.bigdata.datacenter.datasync.service.impl.mysql

import com.avaje.ebean.SqlRow
import com.bigdata.datacenter.datasync.model.mongodb.ParamObj
import com.bigdata.datacenter.datasync.service.impl.AllIndexServiceImplNew
import com.bigdata.datacenter.datasync.service.impl.MailService
import com.bigdata.datacenter.datasync.utils.DateUtils
import com.bigdata.datacenter.datasync.utils.PropertiesUtil
import com.bigdata.datacenter.datasync.utils.constants.MongoDBConfigConstants
import com.bigdata.datacenter.metadata.service.ResultHandler
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Logger

import java.sql.Timestamp

/**
 * Created by Dan on 2017/6/28.
 */
class AllIndexFrDBResultHandler implements ResultHandler {
    private static final Logger logger = Logger.getLogger(AllIndexFrDBResultHandler.class)
    private ParamObj[] result
    private List wtExeInsLst
    private Map propsMap = [:]
    private DBCommon_mysql dbCommon_mysql
    def MailService mailService;

    public AllIndexFrDBResultHandler(ParamObj[] result, Map propsMap, List wtExeInsLst,
                                     DBCommon_mysql dbCommon_mysql) {
        this.result = result
        this.wtExeInsLst = wtExeInsLst
        this.propsMap = propsMap
        this.dbCommon_mysql = dbCommon_mysql
    }

    public AllIndexFrDBResultHandler(ParamObj[] result, Map propsMap, List wtExeInsLst,
                                     DBCommon_mysql dbCommon_mysql, MailService mailService) {
        this.result = result
        this.wtExeInsLst = wtExeInsLst
        this.propsMap = propsMap
        this.dbCommon_mysql = dbCommon_mysql
        this.mailService = mailService;
    }

    @Override
    void execute(SqlRow row) {
        if (row == null || row == "") {
            return;
        }
        String key = ""
        // 组建Key值
        for (int i = 0; i < result.length; i++) {
            String keyType = getProType(result[i].getName(), propsMap)
            // 必要的参数字段值没有在数据源结果集中
            if (keyType == "") {
                //throw new SQLException("Index param is not exist in ds field.");
            }
            if (keyType == "DateTime") {
                Timestamp dateTime = row.getTimestamp(result[i].getName());
                Date val = new Date(dateTime.getTime());
                key += "_" + DateUtils.date2String(val, DateUtils.FORMAT_DATE);
            } else {
                key += "_" + row.get(result[i].getName());
            }
        }
        for (int j = 0; j < wtExeInsLst.size(); j++) {
            Map obj = [:]
            Map indexObj = wtExeInsLst.get(j)
            String fldType = getProType(indexObj.get("fld_name"), propsMap);
            String idxId = indexObj.get("idx_id")
            String keyTmp = idxId + key
            // Key值全部大写
            keyTmp = keyTmp.toUpperCase()
            obj.put(AllIndexServiceImplNew.KEY, keyTmp)
            // 获取指标值
            if (fldType == "Decimal") {
                Double val = row.getDouble(indexObj.get("fld_name"))
                if (val == null) {
                    obj.put(AllIndexServiceImplNew.Value, null)
                } else {
                    obj.put(AllIndexServiceImplNew.Value, val)
                }
            }
            if (fldType == "String") {
                String val = row.getString(indexObj.get("fld_name"))
                if (val == null || val == "") {
                    obj.put(AllIndexServiceImplNew.Value, null)
                } else {
                    obj.put(AllIndexServiceImplNew.Value, val.replaceAll("'", "\\\\'"))
                }
            }
            if (fldType == "DateTime") {
                Timestamp dateTime = row.getTimestamp(indexObj.get("fld_name"))
                if (dateTime == null) {
                    obj.put(AllIndexServiceImplNew.Value, null)
                } else {
                    Date val = new Date(dateTime.getTime())
                    obj.put(AllIndexServiceImplNew.Value, val)
                }
            }
            try {
                String idxTbName = MongoDBConfigConstants.INDEX_ALL_DB
                //如果配置了指标分表
                if (StringUtils.isNotEmpty(PropertiesUtil.getProperty("idx.sharding")) && "true".equalsIgnoreCase(PropertiesUtil.getProperty("idx.sharding"))) {
                    idxTbName = MongoDBConfigConstants.INDEX_SHARDING_TB_NAME_PREFIX + idxId
                }
                // 保存数据
                dbCommon_mysql.saveOrUpdate(idxTbName, obj, [KEY: keyTmp])
            } catch (err) {
                logger.error("指标数据【" + keyTmp + "】保存错误：=====" + obj +" errorInfo:"+err)
                mailService.sendMail("指标数据【" + keyTmp + "】数据保存失败", "专题数据源【" + keyTmp + "】数据保存失败，错误信息为：" + err + ",错误数据为：" + obj)
            }

        }
    }

    private String getProType(String fldName, Map prosMap) {
        String rtn = ""
        Map prop = prosMap.get(fldName)
        if (null != prop) {
            rtn = prop.get("prop_typ")
        }
        return rtn
    }
}
