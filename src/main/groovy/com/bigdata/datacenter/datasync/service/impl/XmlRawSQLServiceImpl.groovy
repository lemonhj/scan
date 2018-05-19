package com.bigdata.datacenter.datasync.service.impl

import com.avaje.ebean.EbeanServer
import com.avaje.ebean.SqlQuery
import com.avaje.ebean.SqlRow
import com.bigdata.datacenter.datasync.core.sql.XmlRawSQLLoader
import com.bigdata.datacenter.datasync.service.XmlRawSQLService
import com.bigdata.datacenter.metadata.service.ResultHandler
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

import java.util.function.Consumer

/**
 * Created by Dean on 2017/5/18.
 */
@Service
class XmlRawSQLServiceImpl implements XmlRawSQLService {
    @Autowired
    XmlRawSQLLoader rawSQLLoader

    @Autowired
    EbeanServer ebeanServer

    @Override
    public void queryRawSqlByKey(String key, Closure callback) {
        def sql = rawSQLLoader.sqlMap.get(key)
        SqlQuery query = ebeanServer.createSqlQuery(sql)
        query.findEach(new Consumer<SqlRow>() {
            @Override
            public void accept(SqlRow row) {
                // process row
                callback(row)
            }
        })
    }

    @Override
    public List<Map<String, Object>> queryRawSqlByKey(String key) {
        def sql = rawSQLLoader.sqlMap.get(key)
        SqlQuery query = ebeanServer.createSqlQuery(sql)
        return query.findList()
    }

    @Override
    public void queryRawSqlByKey(String sqlKey, ResultHandler resultHandler){
        def sql = rawSQLLoader.sqlMap.get(sqlKey)
        SqlQuery query = ebeanServer.createSqlQuery(sql)
//        query.bufferFetchSizeHint = 1000
        query.findEach(new Consumer<SqlRow>() {
            @Override
            public void accept(SqlRow row) {
                // process row
                resultHandler.execute(row)
            }
        })
    }

    @Override
    public void queryRawSqlByKey(String sqlKey, Map params, Closure callback) {
        def sql = rawSQLLoader.sqlMap.get(sqlKey)
        SqlQuery query = ebeanServer.createSqlQuery(sql)

        params.each { key, value->
            query.setParameter(key, value)
        }

        query.findEach(new Consumer<SqlRow>() {
            @Override
            public void accept(SqlRow row) {
                // process row
                callback(row)
            }
        })
    }

    @Override
    public List<Map<String, Object>> queryRawSqlByKey(String sqlKey, Map params) {
        def sql = rawSQLLoader.sqlMap.get(sqlKey)
        SqlQuery query = ebeanServer.createSqlQuery(sql)

        params.each { key, value->
            query.setParameter(key, value)
        }

       return query.findList()
    }

    @Override
    public void queryRawSqlByKey(String sqlKey, Map params, ResultHandler resultHandler) {
        def sql = rawSQLLoader.sqlMap.get(sqlKey)
        SqlQuery query = ebeanServer.createSqlQuery(sql)

        params.each { key, value->
            query.setParameter(key, value)
        }

        query.findEach(new Consumer<SqlRow>() {
            @Override
            public void accept(SqlRow row) {
                // process row
                resultHandler.execute(row)
            }
        })
    }
}
