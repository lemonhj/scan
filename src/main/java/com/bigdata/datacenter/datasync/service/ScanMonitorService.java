package com.bigdata.datacenter.datasync.service;

import com.bigdata.datacenter.datasync.enums.DsTypeEnum;
import com.bigdata.datacenter.datasync.enums.ScanStateEnum;
import com.bigdata.datacenter.datasync.enums.ScanTrigEnum;
import com.bigdata.datacenter.datasync.enums.ScanTypeEnum;

import java.util.Map;

/**
 * desc: 扫描监控service
 *
 * @author haiyangp
 *         date:   2018/1/2
 */
public interface ScanMonitorService {
    /**
     * 初始化数据表,如果不存在扫描监控表，则创建
     */
    Integer initTable();

    /**
     * 添加扫描数据源,开始准备扫描
     *
     * @param dsEngName 数据源英文名
     * @param dsObjId   数据源ID
     * @param scanTyp   扫描类型
     */
    Integer addScanDs(String dsEngName, String dsObjId, ScanTypeEnum scanTyp);

    /**
     * 修改扫描扫描状态
     *
     * @param dsObjId       数据记录id
     * @param scanStateEnum 状态枚举
     */
    Integer updateScanDsStateByLastDsObjId(String dsObjId, ScanStateEnum scanStateEnum);

    /**
     * 获取某数据源扫描记录的最后一条
     *
     * @param dsObjId 数据源ID
     * @return 数据源扫描记录
     */
    Map getLastScanDsByDsObjId(String dsObjId);

    /**
     * 获取最后的触发记录
     */
    Map getLastScanTrig();

    /**
     * 获取当前的触发记录
     */
    Map getCurrentScanTrig();

    /**
     * 初始化扫描进度
     *
     * @param scanDsStatusId 扫描状态ID
     * @param addCount       本次数据扫描-数据总数
     */
    int initScanProcess(Integer scanDsStatusId, Integer addCount);

    /**
     * 添加扫描进度,在原有基础上添加进度
     *
     * @param scanDsStatusId 扫描状态ID
     * @param currentCount   本次数据扫描-数据数
     */
    int addScanProcess(Integer scanDsStatusId, Integer currentCount);

    /**
     * 扫描结束，记录结束时间，并变更扫描状态
     *
     * @param dsObjId 数据源ID
     */
    int endScanDs(String dsObjId);

    /**
     * 添加错误信息
     *
     * @param dsObjId 扫描状态ID
     * @param errMsg  错误信息
     */
    int addErrMsg(String dsObjId, String errMsg);

    /**
     * 添加扫描触发记录
     *
     * @param scanTrigEnum 触发类型
     */
    int addScanTrig(ScanTrigEnum scanTrigEnum);

    /**
     * 添加扫描检测记录
     *
     * @param dsObjId   数据源ID
     * @param dsEngName 数据源英文名
     */
    int addScanDsCheck(String dsObjId, String dsEngName);

    /**
     * 修改扫描检测记录--数据源状态
     *
     * @param dsObjId  数据源ID
     * @param needScan 是否需要扫描
     */
    int updateScanDsCheckState(String dsObjId, boolean needScan);

    /**
     * 添加当前数据源记录数
     *
     * @param dsTypeEnum  数据源类型
     * @param dsObjId     数据源OBJ_ID
     * @param sql         SQL
     * @param recordCount 记录数
     */
    int addDsRecordCount(DsTypeEnum dsTypeEnum, String dsObjId, String sql, Integer recordCount);

    /**
     * 更新当前数据源记录数
     *
     * @param dsObjId 数据源OBJ_ID
     */
    int updateDsDestRecordCount(String dsObjId);

    /**
     * 更新数据源状态--将某种类型的数据源的运行状态进行修改
     *
     * @param currentState 当前数据源状态
     * @param afterScanState   欲修改状态
     * @param rootTrigId    触发根ID/或触发ID
     */
    int updateCurrentAllDsState(ScanStateEnum currentState, ScanStateEnum afterScanState, String rootTrigId);
}
