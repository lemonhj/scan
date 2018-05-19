package com.bigdata.datacenter.datasync.service.impl

import com.bigdata.datacenter.datasync.core.data.MongoTemplatePool
import com.bigdata.datacenter.datasync.service.TxtAndRrpStatusService
import com.bigdata.datacenter.datasync.utils.constants.MongoDBConfigConstants
import com.bigdata.datacenter.datasync.model.mongodb.TxtRrpStatus
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.mongodb.core.MongoOperations
import org.springframework.data.mongodb.core.query.Update
import org.springframework.stereotype.Service

import static org.springframework.data.mongodb.core.query.Criteria.where
import static org.springframework.data.mongodb.core.query.Update.update
import static org.springframework.data.mongodb.core.query.Query.query

/**
 * Created by Dean on 2017/5/22.
 */
@Service
class TxtAndRrpStatusServiceImpl implements TxtAndRrpStatusService{
    @Autowired
    MongoTemplatePool mongoTemplatePool

    /**
     * 更新数据源状态
     * @param dsNm
     * @param status
     */
    @Override
    void updateDsStatus(String dsNm, int status) {
        MongoOperations mongoOps = mongoTemplatePool.getByName(MongoDBConfigConstants.TXT_RRP_STATUS_DB)
        TxtRrpStatus txtRrpStatus = mongoOps.findOne(query(where("DS_NAME").is(dsNm)), TxtRrpStatus.class)

        if (txtRrpStatus == null) {
            txtRrpStatus = new TxtRrpStatus()
            txtRrpStatus.with {
                DS_NAME = dsNm
                DS_UPD_TIME = new Date()
                DS_CRT_TIME = new Date()
                DS_STAS = status
            }

            mongoOps.save(txtRrpStatus)
        } else {
            Update update =new Update()
            update.set("DS_STAS", status)
            update.set("DS_UPD_TIME", new Date())
            mongoOps.updateFirst(query(where("DS_NAME").is(dsNm)), update, TxtRrpStatus.class)
        }
    }
}
