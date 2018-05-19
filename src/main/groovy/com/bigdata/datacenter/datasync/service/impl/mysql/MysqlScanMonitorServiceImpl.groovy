package com.bigdata.datacenter.datasync.service.impl.mysql

import com.avaje.ebean.EbeanServer
import com.bigdata.datacenter.datasync.enums.DsTypeEnum
import com.bigdata.datacenter.datasync.enums.ScanStateEnum
import com.bigdata.datacenter.datasync.enums.ScanTrigEnum
import com.bigdata.datacenter.datasync.enums.ScanTypeEnum
import com.bigdata.datacenter.datasync.server.mysql.MysqlSubjectServer
import com.bigdata.datacenter.datasync.service.ScanMonitorService
import com.bigdata.datacenter.datasync.utils.TrigThreadContext
import com.bigdata.datacenter.datasync.utils.TrigUtils
import com.bigdata.datacenter.datasync.utils.constants.MysqlDBConfigConstants
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

import java.util.concurrent.TimeUnit

/**
 * desc: 扫描监控service
 *
 * @author haiyangp
 * date:   2018/1/2
 */
//@Component("scanMonitorService")
class MysqlScanMonitorServiceImpl implements ScanMonitorService {
    private static final Logger logger = Logger.getLogger(MysqlScanMonitorServiceImpl.class)
    @Autowired
    DBCommon_mysql dbCommon_mysql
    @Autowired
    EbeanServer ebeanServerFormMysql
    @Autowired
    EbeanServer ebeanServer
    /**
     * 错误信息表
     */
    private String TB_scan_ds_err = "SCAN_DS_ERR"
    /**
     * 扫描数据状态表
     */
    private String TB_scan_ds_status = "SCAN_DS_STATUS"
    /**
     * 扫描进度
     */
    private String TB_scan_process = "SCAN_PROCESS"

    /**
     * 扫描触发表
     */
    private String TB_scan_trig = "SCAN_TRIG"

    /**
     * 扫描检测表
     */
    private String TB_scan_ds_check = "SCAN_DS_CHECK"

    @Override
    Integer initTable() {
        ebeanServerFormMysql.createSqlUpdate(MysqlDBConfigConstants.SCAN_DS_ERR_MYSQL).execute()
        ebeanServerFormMysql.createSqlUpdate(MysqlDBConfigConstants.SCAN_DS_STATUS_MYSQL).execute()
        ebeanServerFormMysql.createSqlUpdate(MysqlDBConfigConstants.SCAN_PROCESS_MYSQL).execute()
        ebeanServerFormMysql.createSqlUpdate(MysqlDBConfigConstants.SCAN_TRIG_MYSQL).execute()
        ebeanServerFormMysql.createSqlUpdate(MysqlDBConfigConstants.SCAN_DS_CHECK_MYSQL).execute()
        return 1
    }

    @Override
    Integer addScanDs(String dsEngName, String dsObjId, ScanTypeEnum scanTyp) {
        Map obj = [:]
        obj.put("ds_eng_name", dsEngName)
        obj.put("ds_obj_id", dsObjId)
        obj.put("crt_time", new Date())
        obj.put("upt_time", new Date())
        obj.put("is_delete", 0)
        obj.put("state", ScanStateEnum.WAIT.getCode())
        obj.put("scan_typ", scanTyp.getCode())

//        obj.put("scan_trig_id", MysqlSubjectServer.TRIG_ID)
        obj.put("scan_trig_id", TrigUtils.getCurrentTrigId())

        dbCommon_mysql.insert(TB_scan_ds_status, obj)
        return 1
    }

    @Override
    Integer updateScanDsStateByLastDsObjId(String dsObjId, ScanStateEnum scanStateEnum) {
        def dsObj = getLastScanDsByDsObjId(dsObjId)
//        logger.debug("getLastScanDsByDsObjId dsObjId:{} dsObj:{}", dsObjId, dsObj.toString())
        logger.debug("getLastScanDsByDsObjId " + dsObj.toString() + " dsObjId:" + dsObjId)
        if (dsObj != null) {
            Integer dsObjRowId = dsObj.getInteger("id")
            Map obj = [:]
            obj.put("state", scanStateEnum.getCode())
            obj.put("upt_time", new Date())
            if (scanStateEnum == ScanStateEnum.SCANING) {
                obj.put("begin_time", new Date())
            }
            dbCommon_mysql.update(TB_scan_ds_status, obj, ["id": dsObjRowId])
            return 1
        }
        return 0
    }

    @Override
    Map getLastScanDsByDsObjId(String dsObjId) {
        String sql = "SELECT\n" +
                "\t*\n" +
                "FROM\n" +
                "\tscan_ds_status\n" +
                "WHERE\n" +
                "\tupt_time = (\n" +
                "\t\tSELECT\n" +
                "\t\t\tmax(upt_time)\n" +
                "\t\tFROM\n" +
                "\t\t\tscan_ds_status\n" +
                "\t\tWHERE\n" +
                "\t\t\tds_obj_id = :dsObjId and scan_trig_id = :scanTrigId\n" +
                "\t)and ds_obj_id = :dsObjId and scan_trig_id = :scanTrigId"
//        def findList = ebeanServerFormMysql.createSqlQuery(sql).setParameter("dsObjId", dsObjId).findList()
//        if (findList.size() > 1) {
//            logger.error("")
//        }
        String currentTrigId = TrigUtils.getCurrentTrigId()
        logger.info("getLastScanDsByDsObjId sql:" + sql + "\n dsObjId:" + dsObjId + "\n currentTrigId:" + currentTrigId)
        def sqlRow = ebeanServerFormMysql.createSqlQuery(sql)
                .setParameter("dsObjId", dsObjId)
                .setParameter("scanTrigId", currentTrigId)
                .findUnique()
        return sqlRow
    }

    @Override
    Map getLastScanTrig() {
        String sql = "SELECT\n" +
                "\t*\n" +
                "FROM\n" +
                "\tscan_trig\n" +
                "WHERE\n" +
                "\tupt_time =(\n" +
                "\t\tSELECT\n" +
                "\t\t\tmax(upt_time)\n" +
                "\t\tFROM\n" +
                "\t\t\tscan_trig\n" +
                "\t)"
        def sqlRow = ebeanServerFormMysql.createSqlQuery(sql).findUnique()
        return sqlRow
    }

    @Override
    Map getCurrentScanTrig() {
        String sql = "SELECT\n" +
                "\t*\n" +
                "FROM\n" +
                "\tscan_trig\n" +
                "WHERE scan_trig_id = :scanTrigId"
        def sqlRow = ebeanServerFormMysql.createSqlQuery(sql)
                .setParameter("scanTrigId", MysqlSubjectServer.TRIG_ID)
                .findUnique()
        return sqlRow
    }

    @Override
    int initScanProcess(Integer scanDsStatusId, Integer addCount) {
        Map obj = [:]
        obj.put("scan_ds_status_id", scanDsStatusId)
        obj.put("all_count", addCount)
        obj.put("has_finish", 1)
        dbCommon_mysql.insert(TB_scan_process, obj)
        return 1
    }

    @Override
    int addScanProcess(Integer scanDsStatusId, Integer currentCount) {
        //先查询出当前进度
        Map queryParam = [:]
        queryParam.put("scan_ds_status_id", scanDsStatusId)
        def list = dbCommon_mysql.find(TB_scan_process, queryParam)
        Integer currentExcuteCount = 0
        if (list != null && list.size() == 1) {
            currentExcuteCount = list.get(0).getInteger("excute_count")
        } else {
            logger.error("此数据源进度为空")
            return 0
        }
        if (currentExcuteCount == null) {
            currentExcuteCount = 0
        }
        currentExcuteCount += currentCount
        //更新当前新进度
        Map obj = [:]
        obj.put("excute_count", currentExcuteCount)
        obj.put("upt_time", new Date())
        dbCommon_mysql.update(TB_scan_process, obj, ["scan_ds_status_id": scanDsStatusId])
        return 1
    }

    @Override
    int endScanDs(String dsObjId) {
        //先查询出此数据源信息
        def lastScanDsObj = getLastScanDsByDsObjId(dsObjId)
        if (lastScanDsObj == null) {
            logger.error("此数据源信息为空,记录结束信息失败")
            throw new RuntimeException(dsObjId + " 此数据源信息为空,记录结束信息失败")
            return 0
        }
        Date beginDate = lastScanDsObj.getDate("begin_time")
        long dur_secs = TimeUnit.SECONDS.convert(System.currentTimeMillis() - beginDate.time, TimeUnit.MILLISECONDS)
        Map obj = [:]
        // 0:扫描完成        1:正在扫描        2:扫描终止，异常终止        3:强制执行
        obj.put("state", ScanStateEnum.FINSH.getCode())
        obj.put("dur_secs", dur_secs)
        obj.put("end_time", new Date())
        obj.put("upt_time", new Date())
        dbCommon_mysql.update(TB_scan_ds_status, obj, ["id": lastScanDsObj.id])
        //更新数据源目标表记录数
        updateDsDestRecordCount(dsObjId)
        return 1
    }

    @Override
    int addErrMsg(String dsObjId, String errMsg) {
        def dsRow = getLastScanDsByDsObjId(dsObjId)
        if (dsRow == null) {
            logger.error("信息为空,添加错误信息失败 数据源:" + dsObjId)
            return 0
        }
        Map obj = [:]
        obj.put("scan_ds_status_id", dsRow.getInteger("id"))
        obj.put("err_msg", errMsg)
        obj.put("crt_time", new Date())
        obj.put("upt_time", new Date())
        obj.put("is_delete", 0)
        dbCommon_mysql.insert(TB_scan_ds_err, obj)
        return 1
    }

    @Override
    int addScanTrig(ScanTrigEnum scanTrigEnum) {
        Map obj = [:]
        obj.put("crt_time", new Date())
        obj.put("upt_time", new Date())
        obj.put("is_delete", 0)
        obj.put("trig_typ", scanTrigEnum.getCode())

        if (scanTrigEnum == ScanTrigEnum.INIT) {
            obj.put("scan_trig_id", MysqlSubjectServer.TRIG_ID)
            TrigThreadContext.bindCurrentTrigId(MysqlSubjectServer.TRIG_ID)
        } else {
            //不为初始化的
            if (MysqlSubjectServer.IsFirstRun) {
                obj.put("scan_trig_id", MysqlSubjectServer.TRIG_ID)
                MysqlSubjectServer.IsFirstRun = false
                TrigThreadContext.bindCurrentTrigId(MysqlSubjectServer.TRIG_ID)
            } else {
                obj.put("root_scan_trig_id", MysqlSubjectServer.TRIG_ID)
                //从当前线程中获取本次触发ID
                //添加时，构造该线程中的TRIG_ID
                String newTrigId = TrigUtils.genNewTrigId()
                TrigThreadContext.bindCurrentTrigId(newTrigId)
                obj.put("scan_trig_id", newTrigId)
            }

        }
        dbCommon_mysql.insert(TB_scan_trig, obj)
        return 1
    }

    @Override
    int addScanDsCheck(String dsObjId, String dsEngName) {
        Map obj = [:]
        obj.put("crt_time", new Date())
        obj.put("upt_time", new Date())
        obj.put("is_delete", 0)
        obj.put("scan_typ", ScanTrigEnum.UPDATE.getCode())
        obj.put("ds_eng_name", dsEngName)
        obj.put("ds_obj_id", dsObjId)
        obj.put("scan_trig_id", TrigUtils.getCurrentTrigId())
        obj.put("need_scan", 0)
        //INSERT
        dbCommon_mysql.insert(TB_scan_ds_check, obj)
        return 1
    }

    @Override
    int updateScanDsCheckState(String dsObjId, boolean needScan) {
        Map obj = [:]
        obj.put("need_scan", needScan ? 1 : 0)
        obj.put("upt_time", new Date())
        dbCommon_mysql.update(TB_scan_ds_check, obj, ["scan_trig_id": TrigUtils.getCurrentTrigId(), "ds_obj_id": dsObjId])
        return 1
    }

    @Override
    int addDsRecordCount(DsTypeEnum dsTypeEnum, String dsObjId, String sql, Integer recordCount) {
        //先查询出此数据源信息
        def lastScanDsObj = getLastScanDsByDsObjId(dsObjId)
        if (null == lastScanDsObj) {
            logger.error("[addDsRecordCount] error.cause laseScanDsObj is null.dsObjId:" + dsObjId)
            return 0
        }
        String dsEngName = lastScanDsObj.getString("ds_eng_name")
        Integer dsRecordCount = null
        switch (dsTypeEnum) {
            case DsTypeEnum.STATIC_DS:
            case DsTypeEnum.DYN_DS:
                //静态数据源直接count(sql)
                // 动态数据源,获取INIT_SQL的数量
                String countSql = "select count(*) as \"reordCount\" from (" + sql + ")"
                def count = ebeanServer.createSqlQuery(countSql.toString()).findUnique().getBigDecimal("reordCount")
                dsRecordCount = count
                break
            case DsTypeEnum.SUB_DS:
                //分段数据源直接获取其参数数
                dsRecordCount = recordCount
                break
            default:
                logger.error("unknown dsType error.")
                break
        }
        //查询MYSQL表中某数据源记录数
        String countDsMysqlSql = "select count(*)  as \"mysqlRecordCount\" from " + dsEngName
        Integer mysqlRecordCount = ebeanServerFormMysql.createSqlQuery(countDsMysqlSql).findUnique().getInteger("mysqlRecordCount")
        //将数据源记录数入库
        Map obj = [:]
        obj.put("ds_record_count", dsRecordCount)
        obj.put("dest_record_count", mysqlRecordCount)
        dbCommon_mysql.update(TB_scan_ds_status, obj, ["id": lastScanDsObj.getInteger("id")])
        return 1
    }

    @Override
    int updateDsDestRecordCount(String dsObjId) {
        //先查询出此数据源信息
        def lastScanDsObj = getLastScanDsByDsObjId(dsObjId)
        String dsEngName = lastScanDsObj.getString("ds_eng_name")
        //查询MYSQL表中某数据源记录数
        String countDsMysqlSql = "select count(*)  as \"mysqlRecordCount\" from " + dsEngName
        Integer mysqlRecordCount = ebeanServerFormMysql.createSqlQuery(countDsMysqlSql).findUnique().getInteger("mysqlRecordCount")
        //将数据源记录数入库
        Map obj = [:]
        obj.put("dest_record_count", mysqlRecordCount)
        dbCommon_mysql.update(TB_scan_ds_status, obj, ["id": lastScanDsObj.getInteger("id")])
        return 1
    }

    @Override
    int updateCurrentAllDsState(ScanStateEnum currentState, ScanStateEnum afterScanState, String rootTrigId) {
        String updateSql = "update scan_ds_status set upt_time = NOW() ,state = :afterState where  state = :currentState and scan_trig_id in (\n" +
                "select DISTINCT scan_trig_id from scan_trig where root_scan_trig_id = :trigId,:trigId\n" +
                ")"
        return ebeanServerFormMysql.createSqlUpdate(updateSql)
                .setParameter("afterState", currentState.code)
                .setParameter("currentState", afterScanState.code)
                .setParameter("trigId", rootTrigId)
                .execute()
    }
}
