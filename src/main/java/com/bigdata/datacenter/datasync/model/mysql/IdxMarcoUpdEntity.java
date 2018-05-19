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
@Table(name = "idx_marco_upd", schema = "bigdatadb", catalog = "")
public class IdxMarcoUpdEntity {
    private String dsName;
    private Timestamp maxUpdTime;

    @Basic
    @Column(name = "DS_NAME", nullable = false, length = 256)
    public String getDsName() {
        return dsName;
    }

    public void setDsName(String dsName) {
        this.dsName = dsName;
    }

    @Basic
    @Column(name = "MAX_UPD_TIME", nullable = true)
    public Timestamp getMaxUpdTime() {
        return maxUpdTime;
    }

    public void setMaxUpdTime(Timestamp maxUpdTime) {
        this.maxUpdTime = maxUpdTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IdxMarcoUpdEntity that = (IdxMarcoUpdEntity) o;

        if (dsName != null ? !dsName.equals(that.dsName) : that.dsName != null) return false;
        if (maxUpdTime != null ? !maxUpdTime.equals(that.maxUpdTime) : that.maxUpdTime != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = dsName != null ? dsName.hashCode() : 0;
        result = 31 * result + (maxUpdTime != null ? maxUpdTime.hashCode() : 0);
        return result;
    }
}
