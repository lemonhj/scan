package com.bigdata.datacenter.datasync.service;

import com.bigdata.datacenter.metadata.service.ResultHandler;
import groovy.lang.Closure;

import java.util.List;
import java.util.Map;

/**
 * Created by Dean on 2017/5/18.
 */
public interface XmlRawSQLService {
    /**
     *  通过key找到对应的原生SQL，查询数据，并通过回调闭包返回结果
     * @param key   SQL配置文件对应的ID
     * @param callback  回调闭包
     */
    public void queryRawSqlByKey(String key, Closure callback);

    /**
     * 通过key找到对应的原生SQL，查询数据，查询结果以Map形式返回
     * @param key   SQL配置文件对应的ID
     * @return 返回数据 Map
     */
    public List<Map<String, Object>> queryRawSqlByKey(String key);

    /**
     * 通过key找到对应的原生SQL（可传参SQL），查询数据，并通过回调接口返回结果
     * @param sqlKey    SQL配置文件对应的ID
     * @param resultHandler  回调接口
     */
    public void queryRawSqlByKey(String sqlKey, ResultHandler resultHandler);

    /**
     * 通过key找到对应的原生SQL（可传参SQL），查询数据，并通过回调闭包返回结果
     * @param sqlKey    SQL配置文件对应的ID
     * @param params    参数
     * @param callback  回调闭包
     */
    public void queryRawSqlByKey(String sqlKey, Map params, Closure callback);

    /**
     * 通过key找到对应的原生SQL（可传参SQL），查询结果以Map形式返回
     * @param sqlKey    SQL配置文件对应的ID
     * @param params    参数
     * @return 返回数据 Map
     */
    public List<Map<String, Object>> queryRawSqlByKey(String sqlKey, Map params);

    /**
     * 通过key找到对应的原生SQL（可传参SQL），查询数据，并通过回调闭包返回结果
     * @param sqlKey    SQL配置文件对应的ID
     * @param params    参数
     * @param resultHandler  回调接口
     */
    public void queryRawSqlByKey(String sqlKey, Map params, ResultHandler resultHandler);
}
