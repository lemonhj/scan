package com.bigdata.datacenter.datasync.utils.constants

/**
 * Created by Dean on 2017/5/23.
 */
class MysqlDBConfigConstants {
    //子表更新时间表
    static String SUB_TBL_UPD_TIME_MYSQL = "create table if not exists SUB_TBL_UPD_TIME\n" +
            "(\n" +
            "   TBL_NAME              varchar(255) comment '数据表名称',\n" +
            "   UPD_TIME          datetime comment '数据表更新时间'\n" +
            ")"

    //数据源状态表SQL
    static String DS_STATUS_MYSQL = "create table if not exists DS_STATUS\n" +
            "(\n" +
            "   DS_NAME              varchar(255) comment '数据源名称',\n" +
            "   DS_UPD_TIME          datetime comment '数据源更新时间',\n" +
            "   DS_TYPE              bigint comment '数据源类型',\n" +
            "   DS_STAS              int comment '数据源状态',\n" +
            "   LAST_UPD_TIME        datetime comment '最后更新时间',\n" +
            "   DS_CRT_TIME          datetime comment '数据源创建时间',\n" +
            "   DS_ID                bigint comment '数据源Id'\n" +
            ")";

    //指标数据表sql
    static String IDX_ALL_MYSQL = "create table if not exists IDX_ALL\n" +
            "(\n" +
            "   `KEY`                  varchar(128) comment '指标键',\n" +
            "   `VALUE`                varchar(6000) comment '指标值'\n" +
            ")";

    //指标状态表sql
    static String IDX_STATUS_MYSQL = "create table if not exists IDX_STATUS\n" +
            "(\n" +
            "   DS_NAME              varchar(255) comment '数据源名称',\n" +
            "   DS_UPD_TIME          datetime comment '数据源更新时间',\n" +
            "   DS_TYPE              bigint comment '数据源类型',\n" +
            "   DS_STAS              int comment '数据源状态',\n" +
            "   LAST_UPD_TIME        datetime comment '最后更新时间',\n" +
            "   DS_CRT_TIME          datetime comment '数据源创建时间',\n" +
            "   DS_ID                bigint comment '数据源Id'\n" +
            ")";

    //宏观指标表sql
    static String IDX_MARCO_MYSQL = "create table if not exists IDX_MARCO\n" +
            "(\n" +
            "   IDX_ID               decimal comment '指标Id',\n" +
            "   IDX_VAL              varchar(256) comment '指标值',\n" +
            "   ENDDATE              datetime comment '结束时间',\n" +
            "   UPD_TIME             datetime comment '更新时间'\n" +
            ")";
    //宏观指标更新时间表sql
    static String IDX_MARCO_UPD_MYSQL = "create table if not exists IDX_MARCO_UPD\n" +
            "(\n" +
            "   DS_NAME              varchar(256) comment '数据源名称',\n" +
            "   MAX_UPD_TIME         datetime comment '更新时间'\n" +
            ")";
    //扫描监控--数据源出错表
    static String SCAN_DS_ERR_MYSQL = "create table IF NOT EXISTS scan_ds_err\n" +
            "(\n" +
            "   id                   int not null auto_increment comment '编号',\n" +
            "   scan_ds_status_id    int comment '扫描状态id(外键)',\n" +
            "   err_msg              text comment '错误信息',\n" +
            "   crt_time             datetime comment '创建时间',\n" +
            "   is_delete            smallint comment '是否删除',\n" +
            "   upt_time             datetime comment '更新时间',\n" +
            "   primary key (id)\n" +
            ")"

    //扫描监控--数据源状态表
    static String SCAN_DS_STATUS_MYSQL = "create table IF NOT EXISTS scan_ds_status\n" +
            "(\n" +
            "   id                   int not null auto_increment comment '编号',\n" +
            "   ds_eng_name          varchar(64) comment '数据源英文名',\n" +
            "   ds_obj_id            varchar(32) comment '数据源ID',\n" +
            "   crt_time             datetime comment '创建时间/开始时间',\n" +
            "   upt_time             datetime comment '更新时间',\n" +
            "   is_delete            smallint comment '是否删除\n" +
            "            0:未删除\n" +
            "            1:已删除',\n" +
            "   state                smallint comment '状态\n" +
            "            0:扫描完成\n" +
            "            1:正在扫描\n" +
            "            2:扫描终止，异常终止\n" +
            "            3:强制执行\n" +
            "            4:等待扫描\n" +
            "            5:冲突终止\n" +
            "            6:停止扫描',\n" +
            "   dur_secs             bigint comment '持续时长',\n" +
            "   end_time             datetime comment '结束时间',\n" +
            "   scan_typ             smallint comment '扫描类型\n" +
            "            1:初始化\n" +
            "            2:更新',\n" +
            "   scan_trig_id         varchar(32) comment '扫描触发编号',\n" +
            "   ds_record_count      int comment '数据源记录数',\n" +
            "   dest_record_count    int comment '目标表记录数',\n" +
            "   begin_time           datetime comment '开始时间',\n" +
            "   primary key (id)\n" +
            ")"

    //扫描监控--数据源进度表
    static String SCAN_PROCESS_MYSQL = "create table IF NOT EXISTS scan_process\n" +
            "(\n" +
            "   id                   int not null comment '编号',\n" +
            "   scan_ds_status_id    int comment '扫描状态id(外键)',\n" +
            "   all_count            int comment '总数',\n" +
            "   excute_count         int comment '已扫描数',\n" +
            "   has_finish           smallint comment '是否扫描完毕\n" +
            "            1:未完成\n" +
            "            0:已完成',\n" +
            "   primary key (id)\n" +
            ")"

    //扫描监控----扫描触发表
    static String SCAN_TRIG_MYSQL = "create table IF NOT EXISTS scan_trig\n" +
            "(\n" +
            "   id                   int not null auto_increment comment 'id',\n" +
            "   scan_trig_id         varchar(32) comment '扫描触发编号',\n" +
            "   root_scan_trig_id    varchar(32) comment '根触发ID',\n" +
            "   crt_time             timestamp default CURRENT_TIMESTAMP comment '创建时间',\n" +
            "   upt_time             timestamp comment '更新时间',\n" +
            "   is_delete            tinyint comment '是否删除',\n" +
            "   trig_typ             tinyint comment '触发类型\n" +
            "            0:初始化\n" +
            "            1:更新\n" +
            "            2:定时周新\n" +
            "            3:手动触发',\n" +
            "   note                 varchar(32) comment '备注',\n" +
            "   is_finish             tinyint default 0 comment '是否完成\n" +
            "            0:未完成\n" +
            "            1:已完成',\n" +
            "   primary key (id)" +
            ")"

    //扫描监控----扫描检测表
    static String SCAN_DS_CHECK_MYSQL = "create table IF NOT EXISTS scan_ds_check\n" +
            "(\n" +
            "    id                   int not null auto_increment comment '编号',\n" +
            "   crt_time             timestamp comment '创建时间',\n" +
            "   upt_time             timestamp comment '更新时间',\n" +
            "   is_delete            tinyint comment '是否删除',\n" +
            "   scan_typ             char(1) comment '类型" +
            "             1更新/0初始化',\n" +
            "   ds_eng_name          varchar(64) comment '数据源英文名',\n" +
            "   ds_obj_id            varchar(32) comment '数据源ID',\n" +
            "   scan_trig_id         varchar(32) comment '触发编号',\n" +
            "   need_scan            tinyint comment '是否需要扫描',\n" +
            "   primary key (id)\n" +
            ")"
}
