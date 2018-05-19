package com.bigdata.datacenter.datasync.enums;

/**
 * desc:扫描触发枚举
 *
 * @author haiyangp
 * date:   2018/1/22
 */
public enum ScanTrigEnum {
    /**
     * 初始化
     */
    INIT(0),
    /**
     * 更新
     */
    UPDATE(1),
    /**
     * 定时任务
     */
    CRON(2),
    /**
     * 手动强制刷新
     */
    MANUAL(3);

    private Integer code;

    ScanTrigEnum(Integer code) {
        this.code = code;
    }

    public Integer getCode() {
        return code;
    }
}
