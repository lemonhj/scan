package com.bigdata.datacenter.datasync.model.mysql;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import java.sql.Timestamp;

/**
 * Created by qq on 2017/8/11.
 */
@Entity
@Table(name = "idx_marco", schema = "bigdatadb", catalog = "")
public class IdxMarcoEntity {
    private Integer idxId;
    private String idxVal;
    private Timestamp enddate;
    private Timestamp updTime;

    @Basic
    @Column(name = "IDX_ID", nullable = false, precision = 0)
    public Integer getIdxId() {
        return idxId;
    }

    public void setIdxId(Integer idxId) {
        this.idxId = idxId;
    }

    @Basic
    @Column(name = "IDX_VAL", nullable = true, length = 256)
    public String getIdxVal() {
        return idxVal;
    }

    public void setIdxVal(String idxVal) {
        this.idxVal = idxVal;
    }

    @Basic
    @Column(name = "ENDDATE", nullable = true)
    public Timestamp getEnddate() {
        return enddate;
    }

    public void setEnddate(Timestamp enddate) {
        this.enddate = enddate;
    }

    @Basic
    @Column(name = "UPD_TIME", nullable = true)
    public Timestamp getUpdTime() {
        return updTime;
    }

    public void setUpdTime(Timestamp updTime) {
        this.updTime = updTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IdxMarcoEntity that = (IdxMarcoEntity) o;

        if (idxId != null ? !idxId.equals(that.idxId) : that.idxId != null) return false;
        if (idxVal != null ? !idxVal.equals(that.idxVal) : that.idxVal != null) return false;
        if (enddate != null ? !enddate.equals(that.enddate) : that.enddate != null) return false;
        if (updTime != null ? !updTime.equals(that.updTime) : that.updTime != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = idxId != null ? idxId.hashCode() : 0;
        result = 31 * result + (idxVal != null ? idxVal.hashCode() : 0);
        result = 31 * result + (enddate != null ? enddate.hashCode() : 0);
        result = 31 * result + (updTime != null ? updTime.hashCode() : 0);
        return result;
    }
}
