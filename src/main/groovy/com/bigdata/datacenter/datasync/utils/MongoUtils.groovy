package com.bigdata.datacenter.datasync.utils

import com.mongodb.BasicDBObject
import com.mongodb.DBObject
import org.springframework.data.mongodb.core.MongoOperations

/**
 * Created by Dean on 2017/5/23.
 */
class MongoUtils {

    /**
     * 如果 Collection存在，删除
     * @param mongoTemplate
     * @param clazz
     */
    static void delCollection(MongoOperations mongoTemplate, Class clazz) {
        if (mongoTemplate.collectionExists(clazz)) {
            mongoTemplate.dropCollection(clazz)
        }
    }

    /**
     * 创建索引
     * @param colNm
     * @param dsIndexLst
     */
    static void addIndex(MongoOperations mongoTemplate,String colNm,List<Map<String, Object>> dsIndexLst){
        if(dsIndexLst==null||dsIndexLst.size()==0){
            return
        }
        def idxNm = dsIndexLst.get(0).INDEX_NAME;
        DBObject keys = new BasicDBObject();
        dsIndexLst.each {
            // 索引名称判断
            if (!idxNm.equals(it.INDEX_NAME)) {
                mongoTemplate.getCollection(colNm).createIndex(keys)
                keys = new BasicDBObject();
                idxNm = it.INDEX_NAME;
            }
            if (it.ORDER_RULE == null || it.ORDER_RULE == "") {
                keys.put(it.PROP_NAME, null);
            } else {
                if (it.ORDER_RULE.toLowerCase().equals("asc")) {
                    keys.put(it.PROP_NAME, 1);
                } else if (it.ORDER_RULE.toLowerCase().equals("desc")) {
                    keys.put(it.PROP_NAME, -1);
                }
            }
        }
        // 循环外补充一次操作
        if (keys.keySet().size() != 0) {
            mongoTemplate.getCollection(colNm).createIndex(keys)
        }
    }
}
