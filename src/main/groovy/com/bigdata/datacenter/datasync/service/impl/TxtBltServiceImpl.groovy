package com.bigdata.datacenter.datasync.service.impl

import com.bigdata.datacenter.datasync.core.data.MongoTemplatePool
import com.bigdata.datacenter.datasync.service.ScanService
import com.bigdata.datacenter.datasync.service.TxtAndRrpStatusService
import com.bigdata.datacenter.datasync.service.XmlRawSQLService
import com.bigdata.datacenter.datasync.service.impl.resulthandler.BulletResultHandler
import com.bigdata.datacenter.datasync.utils.DateUtils
import com.bigdata.datacenter.datasync.utils.MongoUtils
import com.bigdata.datacenter.datasync.utils.constants.MongoDBConfigConstants
import com.bigdata.datacenter.datasync.model.mongodb.BulletTemp
import com.bigdata.datacenter.metadata.service.RawSQLService
import groovy.time.TimeCategory
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.stereotype.Service

/**
 * Created by Dean on 2017/5/15.
 */
@Service(value = "txt-blt")
class TxtBltServiceImpl implements ScanService {

    private static final Logger logger = Logger.getLogger(TxtBltServiceImpl.class)

    @Autowired
    RawSQLService rawSQLService

    @Autowired
    XmlRawSQLService xmlRawSQLService

    @Autowired
    MongoTemplatePool mongoTemplatePool

    @Autowired
    TxtAndRrpStatusService txtAndRrpStatusService

    private static Map secuidMap = [:]

    /**
     * 全量更新，即初始化
     */
    @Override
    void totalSync() {
        try {
            if (secuidMap.isEmpty()) {
                setSecuidMap()
            }

            txtAndRrpStatusService.updateDsStatus(MongoDBConfigConstants.TXT_BLT_DB, TxtAndRrpStatusService.DS_STAS_UPDATE)

            MongoOperations txtBltTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.TXT_BLT_DB)
            MongoUtils.delCollection(txtBltTemplate, BulletTemp.class)
            txtBltTemplate.createCollection(BulletTemp.class)

            // 扫描起始日期
            Date begDt = Date.parse(DateUtils.FORMAT_DATE, "1980-01-01")
            Date endDt = Date.parse(DateUtils.FORMAT_DATE, "1980-01-31")

            // 逐年扫描
            while (DateUtils.getYearFrDate(begDt) <= DateUtils.getYearFrDate(new Date())) {
                logger.info("start:" + begDt.format(DateUtils.FORMAT_DATE) + "--end:" + endDt.format(DateUtils.FORMAT_DATE))

                HashMap<String,Date> paramMap = new HashMap<String, Date>()
                begDt = DateUtils.getFirstDateOfMonth(begDt)
                endDt = DateUtils.getLastDateOfMonth(endDt)

                xmlRawSQLService.queryRawSqlByKey("selectAllBltByMon", [
                        begDate: begDt,
                        endDate: endDt
                ], new BulletResultHandler(txtBltTemplate))

                use (TimeCategory) {
                    begDt = begDt + 1.months
                    endDt = endDt + 1.months
                }
            }

        } catch (err) {
            logger.error(err)
            txtAndRrpStatusService.updateDsStatus(MongoDBConfigConstants.TXT_BLT_DB, TxtAndRrpStatusService.DS_STAS_ERROR)
        }

        println("completed")
    }

    /**
     * 回调闭包，将查询数据同步插入到Mongo数据库中
     */
    def callback = { row ->
        print(row.id)
    }

    /**
     * 增量更新
     */
    @Override
    void incrementalSync() {
        MongoOperations txtBltTemplate = mongoTemplatePool.getByName(MongoDBConfigConstants.TXT_BLT_DB)
        def list = txtBltTemplate.findAll(BulletTemp.class)
        println("completed")
    }

    private def setSecuidMap() {
        long start = System.currentTimeMillis()
        logger.info("getSecuidMap" + ":" + "start")
        secuidMap.clear()
        def secuidMapLst = xmlRawSQLService.queryRawSqlByKey("selectSecuIdLst")

        for (item in secuidMapLst) {
            if (item == null) {
                break
            }

            int secu_id = item.SECU_ID as int
            int com_id = item.COM_ID as int

            if (secuidMap.keySet().contains(com_id)) {
                List secuLst = secuidMap.get(com_id)

                if (!secuLst.contains(secu_id)) {
                    secuLst.add(secu_id)
                }
            } else {
                secuidMap.put(com_id, [secu_id])
            }
        }

        long end = System.currentTimeMillis()
        logger.info("getSecuidMap" + ":" + (end-start) + "ms")
    }
}
