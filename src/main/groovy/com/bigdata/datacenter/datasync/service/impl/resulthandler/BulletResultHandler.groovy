package com.bigdata.datacenter.datasync.service.impl.resulthandler

import com.avaje.ebean.SqlRow
import com.bigdata.datacenter.datasync.model.mongodb.BulletTemp
import com.bigdata.datacenter.metadata.service.ResultHandler
import com.bigdata.datacenter.datasync.utils.EntityUtil
import org.springframework.data.mongodb.core.MongoOperations

/**
 * Created by Dean on 2017/5/25.
 */
class BulletResultHandler implements ResultHandler{
    private MongoOperations mongoOperations
    private Map secuidMap = [:]
    private Double id = null
    public List blts = []

    public BulletResultHandler(MongoOperations mongoOperations) {
        this.mongoOperations = mongoOperations
    }

    @Override
    void execute(SqlRow row) {
        BulletTemp bulletTemp = EntityUtil.mapToObject(row, BulletTemp.class)
        try {
            if (id == null) {
                id = bulletTemp.ID
            }

            if (bulletTemp.ID.doubleValue() != id.doubleValue()) {
                saveBlt()
                id = bulletTemp.ID
                blts.clear()
            }
            blts.add(bulletTemp)
        } catch (Exception e) {
//            // 发送邮件错误日志
//            mailService.sendErrLogMail(TxtBltService.class.getName(), "ID=" + result.getID() + ":" + e.toString());
//            logger.error("ID=" + result.getID() + ":" + e.toString());
        }
    }

    void saveBlt() {

    }
}
