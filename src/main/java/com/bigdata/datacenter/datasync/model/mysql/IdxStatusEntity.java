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
@Table(name = "idx_status", schema = "bigdatadb", catalog = "")
public class IdxStatusEntity {
    private String dsName;
    private Timestamp dsUpdTime;
    private Long dsType;
    private Integer dsStas;
    private Timestamp lastUpdTime;
    private Timestamp dsCrtTime;
    private Long dsId;

    @Basic
    @Column(name = "DS_NAME", nullable = false, length = 255)
    public String getDsName() {
        return dsName;
    }

    public void setDsName(String dsName) {
        this.dsName = dsName;
    }

    @Basic
    @Column(name = "DS_UPD_TIME", nullable = true)
    public Timestamp getDsUpdTime() {
        return dsUpdTime;
    }

    public void setDsUpdTime(Timestamp dsUpdTime) {
        this.dsUpdTime = dsUpdTime;
    }

    @Basic
    @Column(name = "DS_TYPE", nullable = false)
    public Long getDsType() {
        return dsType;
    }

    public void setDsType(Long dsType) {
        this.dsType = dsType;
    }

    @Basic
    @Column(name = "DS_STAS", nullable = false)
    public Integer getDsStas() {
        return dsStas;
    }

    public void setDsStas(Integer dsStas) {
        this.dsStas = dsStas;
    }

    @Basic
    @Column(name = "LAST_UPD_TIME", nullable = true)
    public Timestamp getLastUpdTime() {
        return lastUpdTime;
    }

    public void setLastUpdTime(Timestamp lastUpdTime) {
        this.lastUpdTime = lastUpdTime;
    }

    @Basic
    @Column(name = "DS_CRT_TIME", nullable = false)
    public Timestamp getDsCrtTime() {
        return dsCrtTime;
    }

    public void setDsCrtTime(Timestamp dsCrtTime) {
        this.dsCrtTime = dsCrtTime;
    }

    @Basic
    @Column(name = "DS_ID", nullable = false)
    public Long getDsId() {
        return dsId;
    }

    public void setDsId(Long dsId) {
        this.dsId = dsId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IdxStatusEntity that = (IdxStatusEntity) o;

        if (dsName != null ? !dsName.equals(that.dsName) : that.dsName != null) return false;
        if (dsUpdTime != null ? !dsUpdTime.equals(that.dsUpdTime) : that.dsUpdTime != null) return false;
        if (dsType != null ? !dsType.equals(that.dsType) : that.dsType != null) return false;
        if (dsStas != null ? !dsStas.equals(that.dsStas) : that.dsStas != null) return false;
        if (lastUpdTime != null ? !lastUpdTime.equals(that.lastUpdTime) : that.lastUpdTime != null) return false;
        if (dsCrtTime != null ? !dsCrtTime.equals(that.dsCrtTime) : that.dsCrtTime != null) return false;
        if (dsId != null ? !dsId.equals(that.dsId) : that.dsId != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = dsName != null ? dsName.hashCode() : 0;
        result = 31 * result + (dsUpdTime != null ? dsUpdTime.hashCode() : 0);
        result = 31 * result + (dsType != null ? dsType.hashCode() : 0);
        result = 31 * result + (dsStas != null ? dsStas.hashCode() : 0);
        result = 31 * result + (lastUpdTime != null ? lastUpdTime.hashCode() : 0);
        result = 31 * result + (dsCrtTime != null ? dsCrtTime.hashCode() : 0);
        result = 31 * result + (dsId != null ? dsId.hashCode() : 0);
        return result;
    }
}
