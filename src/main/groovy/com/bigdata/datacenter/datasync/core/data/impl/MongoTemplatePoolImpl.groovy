package com.bigdata.datacenter.datasync.core.data.impl

import com.bigdata.datacenter.datasync.core.data.MongoTemplatePool
import com.mongodb.Mongo
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.stereotype.Service

/**
 * Created by Dean on 2017/5/22.
 */
@Service
class MongoTemplatePoolImpl implements MongoTemplatePool{
    @Autowired
    Mongo mongo

    MongoTemplate getByName(String name) {
        return new MongoTemplate(mongo, name)
    }

    List<String> getDbBaseNames(){
        return mongo.getDatabaseNames()
    }

    void dropDbBase(String dbNm){
        mongo.dropDatabase(dbNm)
    }
}
