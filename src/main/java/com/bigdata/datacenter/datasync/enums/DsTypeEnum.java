package com.bigdata.datacenter.datasync.enums;

/**
 * desc:数据源类型枚举
 * 2静态、3分段、7动态
 *
 * @author haiyangp
 *         date:   2018/3/12
 */
public enum DsTypeEnum {
    /**
     * 静态数据源
     */
    STATIC_DS(2),
    /**
     * 分段数据源
     */
    SUB_DS(3),
    /**
     * 动态数据源
     */
    DYN_DS(7);

    private Integer code;

    DsTypeEnum(Integer code) {
        this.code = code;
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }
}
