package com.bigdata.datacenter.datasync.enums;

/**
 * desc:扫描类型枚举
 * 初始化/更新
 *
 * @author haiyangp
 * date:   2018/1/22
 */
public enum ScanTypeEnum {
    /**
     * 初始化
     */
    INIT(0),
    /**
     * 更新
     */
    UPDATE(1);
    private Integer code;

    ScanTypeEnum(Integer code) {
        this.code = code;
    }

    public Integer getCode() {
        return code;
    }
}
