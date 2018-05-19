package com.bigdata.datacenter.datasync.enums;

/**
 * desc:扫描状态枚举
 * 扫描完成/正在扫描/扫描终止，异常终止/强制执行/等待扫描
 *
 * @author haiyangp
 *         date:   2018/1/22
 */
public enum ScanStateEnum {
    /**
     * 扫描完成
     */
    FINSH(0),
    /**
     * 正在扫描
     */
    SCANING(1),
    /**
     * 扫描终止，异常终止
     */
    ERR(2),
    /**
     * 强制执行
     */
    EXIT(3),
    /**
     * 等待扫描
     */
    WAIT(4),
    /**
     * 冲突--退出
     */
    CONFLICT_EXIT(5),
    /**
     * 程序退出
     */
    APP_EXIT(6);
    private Integer code;

    ScanStateEnum(Integer code) {
        this.code = code;
    }

    public Integer getCode() {
        return code;
    }
}
