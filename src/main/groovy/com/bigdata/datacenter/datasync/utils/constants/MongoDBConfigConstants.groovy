package com.bigdata.datacenter.datasync.utils.constants

/**
 * Created by Dean on 2017/5/23.
 */
class MongoDBConfigConstants {
    public static final String[] XML_FILES = [
            "applicationContext-dataSource.xml",
            "applicationContext-ebean.xml",
            "applicationContext-mongo.xml",
            "applicationContext-spring.xml"];

    // 更新操作
    public static final String MAIN_PARAM_UPD = "-U";
    // 插入操作
    public static final String MAIN_PARAM_INS = "-I";

    // 系统库
    public static final String ADMIN = "admin";
    public static final String LOCAL = "local";

    // 指标数据库名称
    static String INDEX_ALL_DB = "IDX_ALL";
    // 指标-分表表名-前缀
    static String INDEX_SHARDING_TB_NAME_PREFIX = "SUB_IDX_";
    // 数据源中心库关联表-更新字段
    static String DS_CENTER_TB_UPD_COULMN = "DS_CENTER_TB_UPD_COULMN";
    // 公告数据库名称
    static String TXT_BLT_DB = "TXT_BLT_LIST";
    // 资讯数据库名称
    static String TXT_NWS_DB = "TXT_NWS_HOME";
    // 微财经数据库名称
    static String TXT_WCJ_DB = "TXT_WCJ_HOME";
    // 一财内参
    static String TXT_YCNC_DB = "TXT_YCNC_HOME";
    // 法律法规数据库名称
    static String TXT_LAW_DB = "TXT_LAWS_LIST";
    // 提示信息数据库名称
    static String TXT_TIP_DB = "TXT_TIPS_LIST";
    // 研究报告数据库名称
    static String RRP_BAS_DB = "RRP_RPT_BAS";
    // 专题数据库状态
    static String DS_STATUS_DB = "DS_STATUS";
    // 指标数据库状态
    static String IDX_STATUS_DB = "IDX_STATUS";
    // 公告，新闻，研究报告数据库状态
    static String TXT_RRP_STATUS_DB = "TXT_STATUS";
    // 扫描各表上次更新时间
    static String SUB_TBL_UPD_TIME = "SUB_TBL_UPD_TIME";
    // 扫描各表上次更新时间
    static String IDX_TBL_UPD_TIME = "IDX_TBL_UPD_TIME";
    // 宏观指标
    static String IDX_MARCO = "IDX_MARCO";
    // 宏观指标Max_UPD_TIME
    static String IDX_MARCO_UPD = "IDX_MARCO_UPD";
    // 专题静态和增量数据
    static String SUB_DATA_SOURCE = "SUB_DATA_SOURCE";
    // mongo同步数据操作类型
    static String UPD_TYPE = "update";
    static String INS_TYPE = "insert";
    static String DEL_TYPE = "delete";
}
