package com.bigdata.datacenter.datasync.service.impl.mysql

import com.bigdata.datacenter.datasync.model.mongodb.SubjectStatus
import com.bigdata.datacenter.datasync.service.SubjectStatusService
import com.bigdata.datacenter.datasync.utils.constants.MongoDBConfigConstants
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

/**
 * mysql版
 * Created by haiyang on 2017/11/20.
 */
//@Component("mysql-subjectStatusService")
class MysqlSubjectStatusServiceImpl implements SubjectStatusService {
    private static final Logger logger = Logger.getLogger(MysqlSubjectStatusServiceImpl.class)

    @Autowired
    DBCommon_mysql dbCommon_mysql

    /**
     * 获取全部的数据源状态信息
     * @return
     */
    @Override
    List<SubjectStatus> findAll() {
        List<SubjectStatus> statusList = []
        def list = dbCommon_mysql.find(MongoDBConfigConstants.DS_STATUS_DB, [:])
        if (list != null && list.size() > 0) {
            for (row in list) {
                statusList.add(new SubjectStatus(
                        DS_ID: row.getLong("DS_ID"),
                        DS_NAME: row.getString("DS_NAME"),
                        DS_TYPE: row.getLong("DS_TYPE"),
                        DS_UPD_TIME: row.getDate("DS_UPD_TIME"),
                        DS_CRT_TIME: row.getDate("DS_CRT_TIME"),
                        DS_STAS: row.getInteger("DS_STAS"),
                        LAST_UPD_TIME: row.getDate("LAST_UPD_TIME")
                ))
            }
        } else {
            logger.error("此数据源状态表为空")
        }
        return statusList
    }
    /**
     * 根据数据源名称查询数据源信息
     * @param dsNm
     * @return
     */
    @Override
    SubjectStatus getSubStatusByDsName(String dsNm) {
        def list = dbCommon_mysql.find(MongoDBConfigConstants.DS_STATUS_DB, ["DS_NAME": dsNm])
        if (list != null && list.size() > 0) {
            def row = list.get(0)
            return new SubjectStatus(
                    DS_ID: row.getLong("DS_ID"),
                    DS_NAME: row.getString("DS_NAME"),
                    DS_TYPE: row.getLong("DS_TYPE"),
                    DS_UPD_TIME: row.getDate("DS_UPD_TIME"),
                    DS_CRT_TIME: row.getDate("DS_CRT_TIME"),
                    DS_STAS: row.getInteger("DS_STAS"),
                    LAST_UPD_TIME: row.getDate("LAST_UPD_TIME")
            )
        } else {
            logger.error("此数据源[" + dsNm + "]不存在")
        }
        return null
    }
    /**
     * 获取数据源状态
     * @param dsNm
     * @return
     */
    @Override
    int getDsStatus(String dsNm) {
        Map queryMap = ["DS_NAME": dsNm]
        def list = dbCommon_mysql.find(MongoDBConfigConstants.DS_STATUS_DB, queryMap)
        if (list != null && list.size() > 0) {
            return list.get(0).getInteger("DS_STAS")
        } else {
            logger.error("此数据源[" + dsNm + "]不存在")
        }
        //XXX  待校验
        return 0
    }
    /**
     * 更新数据源状态
     * @param dds
     * @param status
     * @param lastUpdTime
     */
    @Override
    void updateDsStatus(Map<String, Object> dds, int status, Date lastUpdTime) {
        //查询这个数据源的状态是否存在，没有存在，则新增；存在则更新
        def rows = dbCommon_mysql.find(MongoDBConfigConstants.DS_STATUS_DB, ["DS_NAME": dds.DS_ENG_NAME])
        if (rows == null || rows.size() == 0) {
            //SAVE
            Map obj = [:]
            obj.put("DS_ID", dds.OBJ_ID)
            obj.put("DS_NAME", dds.DS_ENG_NAME)
            obj.put("DS_TYPE", dds.DS_TYP)
            obj.put("DS_UPD_TIME", new Date())
            obj.put("DS_CRT_TIME", new Date())
            obj.put("DS_STAS", status)
            obj.put("LAST_UPD_TIME", new Date())
            dbCommon_mysql.insert(MongoDBConfigConstants.DS_STATUS_DB, obj)
        } else {
            //UPDATE
            Map map = [:]
            map.put("DS_STAS", status)
            if (lastUpdTime == null) {
                lastUpdTime = new Date()
            }
            map.put("LAST_UPD_TIME", lastUpdTime)
            dbCommon_mysql.update(MongoDBConfigConstants.DS_STATUS_DB, map, ["DS_NAME": dds.DS_ENG_NAME])
        }
    }
    /**
     * 重置数据源状态
     * @param dds
     * @param status
     * @param lastUpdTime
     */
    @Override
    void resetDsStatus(int oldStatus, int status) {
        Map map = ["DS_STAS": status]
        dbCommon_mysql.update(MongoDBConfigConstants.DS_STATUS_DB, map, ["DS_STAS": oldStatus])
    }

    /**
     * 删除数据源状态
     * @param dsName
     */
    @Override
    void removeDsStatus(String dsName) {
        dbCommon_mysql.delete(MongoDBConfigConstants.DS_STATUS_DB, ["DS_NAME": dsName])
    }
}
