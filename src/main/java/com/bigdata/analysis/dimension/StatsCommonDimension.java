package com.bigdata.analysis.dimension;

import com.bigdata.analysis.dimension.base.BaseDimension;
import com.bigdata.analysis.dimension.base.DateDimension;
import com.bigdata.analysis.dimension.base.KpiDimension;
import com.bigdata.analysis.dimension.base.PlatformDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @Author Taten
 * @Description 公共维度类的封装
 **/
public class StatsCommonDimension extends BaseStatsDimension {
    private DateDimension dateDimension = new DateDimension();
    private PlatformDimension platformDimension = new PlatformDimension();
    private KpiDimension kpiDimension = new KpiDimension();

    public StatsCommonDimension() {
    }

    public StatsCommonDimension(DateDimension dateDimension, PlatformDimension platformDimension, KpiDimension kpiDimension) {
        this.dateDimension = dateDimension;
        this.platformDimension = platformDimension;
        this.kpiDimension = kpiDimension;
    }

    public DateDimension getDateDimension() {
        return dateDimension;
    }

    public void setDateDimension(DateDimension dateDimension) {
        this.dateDimension = dateDimension;
    }

    public PlatformDimension getPlatformDimension() {
        return platformDimension;
    }

    public void setPlatformDimension(PlatformDimension platformDimension) {
        this.platformDimension = platformDimension;
    }

    public KpiDimension getKpiDimension() {
        return kpiDimension;
    }

    public void setKpiDimension(KpiDimension kpiDimension) {
        this.kpiDimension = kpiDimension;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StatsCommonDimension)) return false;
        StatsCommonDimension that = (StatsCommonDimension) o;
        return Objects.equals(getDateDimension(), that.getDateDimension()) &&
                Objects.equals(getPlatformDimension(), that.getPlatformDimension()) &&
                Objects.equals(getKpiDimension(), that.getKpiDimension());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getDateDimension(), getPlatformDimension(), getKpiDimension());
    }


    @Override
    public int compareTo(BaseDimension o) {
        if (o == this) {
            return 0;
        }
        StatsCommonDimension other = (StatsCommonDimension) o;
        int tmp = this.dateDimension.compareTo(other.dateDimension);
        if (0 != tmp) {
            return tmp;
        }
        tmp = this.platformDimension.compareTo(other.platformDimension);
        if (0 != tmp) {
            return tmp;
        }
        tmp = this.kpiDimension.compareTo(other.kpiDimension);
        return tmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.dateDimension.write(out); //对象的写出
        this.platformDimension.write(out);
        this.kpiDimension.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.dateDimension.readFields(in);
        this.platformDimension.readFields(in);
        this.kpiDimension.readFields(in);
    }

    /**
     * 克隆当前对象的一个实例
     *
     * @param dimension
     * @return
     */
    public static StatsCommonDimension clone(StatsCommonDimension dimension) {
        DateDimension dateDimension = new DateDimension(
                dimension.dateDimension.getId(),
                dimension.dateDimension.getYear(),
                dimension.dateDimension.getSeason(),
                dimension.dateDimension.getMonth(),
                dimension.dateDimension.getWeek(),
                dimension.dateDimension.getDay(),
                dimension.dateDimension.getCalendar(),
                dimension.dateDimension.getType()
        );
        PlatformDimension platformDimension = new PlatformDimension(
                dimension.platformDimension.getId(),
                dimension.platformDimension.getPlatformName()
        );
        KpiDimension kpiDimension = new KpiDimension(
                dimension.kpiDimension.getId(),
                dimension.kpiDimension.getKpiName()
        );
        return new StatsCommonDimension(dateDimension, platformDimension, kpiDimension);
    }
}
