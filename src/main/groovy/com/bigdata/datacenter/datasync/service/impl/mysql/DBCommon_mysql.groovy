package com.bigdata.datacenter.datasync.service.impl.mysql

import com.avaje.ebean.EbeanServer
import com.avaje.ebean.SqlUpdate
import com.avaje.ebean.Transaction
import com.bigdata.datacenter.datasync.utils.DateUtils
import com.bigdata.datacenter.datasync.utils.MysqlUtils
import com.bigdata.datacenter.datasync.utils.PropertiesUtil
import com.bigdata.datacenter.datasync.utils.constants.MongoDBConfigConstants
import com.bigdata.datacenter.datasync.utils.constants.MysqlDBConfigConstants
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Logger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service

/** mysql通用工具
 *
 * Created by qq on 2017/8/14.
 */
//@Service
class DBCommon_mysql {
    private static final Logger logger = Logger.getLogger(DBCommon_mysql.class)
    @Autowired
    EbeanServer ebeanServerFormMysql;

    static Map<String, List> colsMap = [:];
    //创建表结构
    private void createTabel(String tabelName) {
        String createTabelSql
        if (tabelName.toUpperCase().startsWith(MongoDBConfigConstants.INDEX_SHARDING_TB_NAME_PREFIX)) {
            //将表名由IDX_ALL改为新的分表表名
            createTabelSql = (MysqlDBConfigConstants.(MongoDBConfigConstants.INDEX_ALL_DB + "_MYSQL")).replaceAll(MongoDBConfigConstants.INDEX_ALL_DB, tabelName)
        } else {
            createTabelSql = MysqlDBConfigConstants.(tabelName.toUpperCase() + "_MYSQL");
        }
        logger.trace("-------------------------->sql is:" + createTabelSql + "  tableName:" + tabelName)
        ebeanServerFormMysql.createSqlUpdate(createTabelSql).execute();
    }

    /**
     * 获取表的全部列名
     * <br>
     */
    private List getColsByTabel(String tabelName) {
        if (!colsMap.containsKey(tabelName)) {
            String sql = "select COLUMN_NAME from INFORMATION_SCHEMA.Columns where TABLE_NAME= '" + tabelName.toUpperCase() + "'" +
                    " AND TABLE_SCHEMA =( SELECT\n" +
                    "\tDATABASE ())"
            def list = ebeanServerFormMysql.createSqlQuery(sql).findList();
            colsMap.put(tabelName, list);
            return list;
        } else {
            return colsMap.get(tabelName);
        }
    }

    //创建索引
    void createIndex(String tabelName, List<String[]> idxCols) {
        createTabel(tabelName);
        String idxName = tabelName.toUpperCase();
        String cols = "";
        for (String[] idxs : idxCols) {
            idxs.each {
                idxName += "_" + it.toUpperCase();
                cols += (",`" + it + "`");
            }
            cols = cols.replaceFirst(",", "");
            String sql = "CREATE INDEX " + idxName + " ON " + tabelName + " (" + cols + ");";
            try {
                ebeanServerFormMysql.createSqlUpdate(sql).execute();
            } catch (e) {
            }
        }
    }

    /**
     * 信息修改
     * @param tabelName 数据库表名
     * @param obj 需要修改的对象
     * @param opType 操作类型
     * @param whereParam 过滤条件
     */
    void saveOrUpdate(String tabelName, Object obj, Map<String, Object> whereParam) {
        createTabel(tabelName);
        def list = find(tabelName, whereParam);
        if (list == null || list.size() == 0) {
            insert(tabelName, obj);
            return;
        } else if (list != null && list.size() > 0) {
            update(tabelName, obj, whereParam);
            return;
        }
    }

    /**
     * 保存数据（不建表）
     * @param tabelName 表名
     * @param obj 数据
     */
    void save(String tabelName, Object obj) {
        List cols = getColsByTabel(tabelName);
        StringBuffer colsBuf = new StringBuffer();
        StringBuffer valsBuf = new StringBuffer();
        if (obj instanceof Map) {
            cols.each {
                obj.each { key, value ->
                    if (key.toUpperCase() == it.column_name.toUpperCase()) {
                        if (value instanceof Date) {
                            value = DateUtils.date2String(value, DateUtils.FORMAT_DATETIME)
                        } else if (value instanceof String) {
                            value = value.replaceAll("'", "\\\\'")
                        }
                        if (value != null) {
                            colsBuf.append(",`" + it.column_name.toUpperCase() + "`");
                            valsBuf.append(",'" + value + "'");
                        }
                    }
                }
            }
        } else {
            cols.each {
                obj.properties.each { key, value ->
                    if (key.toUpperCase() == it.column_name.toUpperCase()) {
                        if (value instanceof Date) {
                            value = DateUtils.date2String(value, DateUtils.FORMAT_DATETIME)
                        } else if (value instanceof String) {
                            value = value.replaceAll("'", "\\\\'")
                        }

                        if (value != null) {
                            colsBuf.append(",`" + it.column_name.toUpperCase() + "`");
                            valsBuf.append(",'" + value + "'");
                        }
                    }
                }
            }
        }
        colsBuf.replace(0, 1, "");
        valsBuf.replace(0, 1, "");
        StringBuffer insertSql = new StringBuffer(" insert into " + tabelName);
        insertSql.append("(").append(colsBuf).append(")").append("values(").append(valsBuf).append(")");
        SqlUpdate sqlUpdate = ebeanServerFormMysql.createSqlUpdate(insertSql.toString());
        try {
            Transaction t = ebeanServerFormMysql.beginTransaction();
            t.setBatchMode(true);
            t.setBatchSize(3);
            sqlUpdate.execute();
            ebeanServerFormMysql.commitTransaction();
        } finally {
            ebeanServerFormMysql.endTransaction();
        }
    }

    //新增
    void insert(String tabelName, Object obj) {
        createTabel(tabelName);
        List cols = getColsByTabel(tabelName);
        StringBuffer colsBuf = new StringBuffer();
        StringBuffer valsBuf = new StringBuffer();
        if (obj instanceof Map) {
            cols.each {
                obj.each { key, value ->
                    if (key.toUpperCase() == it.column_name.toUpperCase()) {
                        if (value instanceof Date) {
                            value = DateUtils.date2String(value, DateUtils.FORMAT_DATETIME)
                        } else if (value instanceof String) {
                            value = value.replaceAll("'", "\\\\'")
                        }

                        if (value != null) {
                            colsBuf.append(",`" + it.column_name.toUpperCase() + "`");
                            valsBuf.append(",'" + value + "'");
                        }
                    }
                }
            }
        } else {
            cols.each {
                obj.properties.each { key, value ->
                    if (key.toUpperCase() == it.column_name.toUpperCase()) {
                        if (value instanceof Date) {
                            value = DateUtils.date2String(value, DateUtils.FORMAT_DATETIME)
                        } else if (value instanceof String) {
                            value = value.replaceAll("'", "\\\\'")
                        }
                        if (value != null) {
                            colsBuf.append(",`" + it.column_name.toUpperCase() + "`");
                            valsBuf.append(",'" + value + "'");
                        }
                    }
                }
            }
        }
        colsBuf.replace(0, 1, "");
        valsBuf.replace(0, 1, "");
        StringBuffer insertSql = new StringBuffer(" insert into " + tabelName);
        insertSql.append("(").append(colsBuf).append(")").append("values(").append(valsBuf).append(")");
        SqlUpdate sqlUpdate = ebeanServerFormMysql.createSqlUpdate(insertSql.toString());
        try {
            Transaction t = ebeanServerFormMysql.beginTransaction();
            t.setBatchMode(true);
            t.setBatchSize(3);
            sqlUpdate.execute();
            ebeanServerFormMysql.commitTransaction();
        } finally {
            ebeanServerFormMysql.endTransaction();
        }
    }

    //修改
    void update(String tabelName, Object obj, Map whereParam) {
        createTabel(tabelName);
        List cols = getColsByTabel(tabelName);
        if (obj == null) {
            return;
        }
        StringBuffer upSql = new StringBuffer("update " + tabelName + " set ");
        if (obj instanceof Map) {
            cols.each {
                obj.each { key, value ->
                    if (key.toUpperCase() == it.column_name.toUpperCase()) {
                        if (value instanceof Date) {
                            value = DateUtils.date2String(value, DateUtils.FORMAT_DATETIME)
                        }
                        if (value != null) {
                            upSql.append("`" + it.column_name.toUpperCase() + "`").append("=").append("'" + value + "'").append(",");
                        }
                    }
                }
            }
        } else {
            cols.each {
                obj.properties.each { key, value ->
                    if (key.toUpperCase() == it.column_name.toUpperCase()) {
                        if (value instanceof Date) {
                            value = DateUtils.date2String(value, DateUtils.FORMAT_DATETIME)
                        }
                        if (value != null) {
                            upSql.append("`" + it.column_name.toUpperCase() + "`").append("=").append("'" + value + "'").append(",");
                        }
                    }
                }
            }
        }
        upSql = upSql.replace(upSql.length() - 1, upSql.length(), " ");

        //设置查询条件
        String whereSql = getWhereParam(whereParam);
        upSql.append(" where ").append(whereSql);
        ebeanServerFormMysql.createSqlUpdate(upSql.toString()).execute();
    }

    //删除
    void delete(String tabelName, Map whereParam) {
        createTabel(tabelName);
        String whereSql = getWhereParam(whereParam);
        StringBuffer delSql = new StringBuffer();
        delSql.append("delete from ")
                .append(tabelName)
                .append(" where ")
                .append(whereSql);
        ebeanServerFormMysql.createSqlUpdate(delSql.toString()).execute();
    }

    //删除,不判断表是否存在
    void deleteByParam(String tabelName, Map whereParam) {
        String whereSql = getWhereParam(whereParam)
        StringBuffer delSql = new StringBuffer()
        delSql.append("delete from ")
                .append(tabelName)
                .append(" where ")
                .append(whereSql)
        logger.debug("deleteData sqlIs:" + delSql)
        ebeanServerFormMysql.createSqlUpdate(delSql.toString()).execute()
    }

    //单表查询
    List find(String tabelName, Map whereParam) {
        createTabel(tabelName);
        String whereSql = getWhereParam(whereParam);
        StringBuffer findSql = new StringBuffer();
        findSql.append("select * from ").append(tabelName).append(" where ").append(whereSql);
        def list = ebeanServerFormMysql.createSqlQuery(findSql.toString()).findList();
        return list;
    }

    //单表查询
    List findByWhereSql(String tabelName, String whereSql) {
        createTabel(tabelName);
        StringBuffer findSql = new StringBuffer();
        findSql.append("select * from ").append(tabelName).append(" where ").append(whereSql);
        def list = ebeanServerFormMysql.createSqlQuery(findSql.toString()).findList();
        return list;
    }

    //拼接过滤参数
    String getWhereParam(Map whereParam) {
        //设置查询条件
        StringBuffer whereSql = new StringBuffer();
        if (whereParam != null) {
            whereParam.each { key, value ->
                whereSql.append("and ").append("`" + key.toUpperCase() + "`").append(" = ").append("'" + value + "'");
            }
        }
        return whereSql.toString().replaceFirst("and", " ");
    }

    /**
     * 建表建索引
     * @param tableName 表名
     * @param props 表字段属性
     * @param dsIndexLst 表索引
     */
    void createTableIndex(String tableName, List<Map<String, Object>> props, List<Map<String, Object>> idxCols) {
        List<String[]> indexArrayList = MysqlUtils.getIndexArray(idxCols)
        createTableWithProps(tableName, props)
        String idxName = tableName.toUpperCase()
        String cols = ""
        for (String[] idxs : indexArrayList) {
            idxs.each {
                idxName += "_" + it.toUpperCase()
                cols += (",`" + it + "`")
            }
            cols = cols.replaceFirst(",", "")
            String sql = "CREATE INDEX " + idxName + " ON " + tableName + " (" + cols + ");"
            try {
                ebeanServerFormMysql.createSqlUpdate(sql).execute();
            } catch (e) {
            }
        }
    }

    /**
     * 创建表结构
     * @param tableName 表名
     * @param props 属性列表
     */
    @SuppressWarnings("GroovyAssignabilityCheck")
    void createTableWithProps(String tableName, List<Map<String, Object>> props) {
        if (props == null || props.size() == 0) {
            logger.error("create Table error.props can not is null.")
            return
        }
        String createTabelSql
        StringBuilder sqlBuilder = new StringBuilder()
        sqlBuilder.append("create table if not exists ")
        sqlBuilder.append(tableName)
        sqlBuilder.append("(")
        //region 遍历属性，拼接SQL
        for (Map<String, Object> propMap in props) {
            //添加字段名
            sqlBuilder.append(propMap.PROP_NAME + " ")
            //添加字段属性类型
            if (propMap.PROP_LEN != null) {
                sqlBuilder.append(getFieldType(propMap.PROP_TYP, Integer.valueOf(propMap.PROP_LEN + "")))
            } else {
                sqlBuilder.append(getFieldType(propMap.PROP_TYP, null))
            }
            //添加字段注释信息
            sqlBuilder.append(" comment '" + propMap.PROP_CAP + "'")
            //添加分隔符
            sqlBuilder.append(",")
        }
        //endregion
        sqlBuilder.append(")")
        //指定数据表引擎
        if (StringUtils.isNotEmpty(PropertiesUtil.getProperty("sub.mysql.engine"))) {
            sqlBuilder.append("ENGINE=")
            sqlBuilder.append(PropertiesUtil.getProperty("sub.mysql.engine").trim())
        }
        //去掉最后一个字符的分隔符
        createTabelSql = sqlBuilder.toString().replace(",)", ")")
        logger.debug("-------------------------->sql is:\n" + createTabelSql + "\n  tableName:" + tableName)
        ebeanServerFormMysql.createSqlUpdate(createTabelSql).execute();
        //建表后删除缓存的表字段数据，否则不能实时更新表结构数据
        colsMap.remove(tableName)
    }

    /**
     * 获取字段类型
     * @param propType 字段类型
     * @param proplength 字段长度
     */
    String getFieldType(String propType, Integer proplength) {
        if ("String" == propType) {
            if (proplength != null && proplength == 0) {
                return "TEXT"
            } else if (proplength == null || proplength == -1) {
                //如果String类型的长度为null，默认设为200
                proplength = 200
            }
            return "VARCHAR(" + proplength + ")"
        } else if ("Decimal" == propType) {
            //小数全为6位,长度默认为16+6位
            if (proplength == null || proplength == -1) {
                return "DECIMAL(22,6)"
                //长度为0 代表小数位数为0,长度为16
            } else if (proplength == 0) {
                return "DECIMAL(16,0)"
            }
            return "DECIMAL(" + proplength + ",6)"
        } else if ("DateTime" == propType) {
            return "DATETIME"
        } else {
            logger.error("字段类型不支持.")
            throw new UnsupportedOperationException("字段类型不支持")
        }
    }

    /**
     * 删除数据表
     * @param tableName
     */
    void dropTable(String tableName) {
        try {
            if (tableName == null || StringUtils.isEmpty(tableName)) {
                throw new RuntimeException("被删除的表名不能为空")
            }
            logger.info("删除数据表:" + tableName)
            String dropSql = "drop table " + tableName
            ebeanServerFormMysql.createSqlUpdate(dropSql).execute()
        } catch (err) {
            logger.error("drop table exception.error info:" + err.getMessage())
        }
    }
}
