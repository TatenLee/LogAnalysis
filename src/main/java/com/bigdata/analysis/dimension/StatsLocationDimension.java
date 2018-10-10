package com.bigdata.analysis.dimension;

import com.bigdata.analysis.dimension.base.BaseDimension;
import com.bigdata.analysis.dimension.base.LocationDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @Author Taten
 * @Description 封装地域模块中map和reduce阶段输出的key的类型
 **/
public class StatsLocationDimension extends BaseStatsDimension {
    private StatsCommonDimension statsCommonDimension = new StatsCommonDimension();
    private LocationDimension locationDimension = new LocationDimension();

    public StatsLocationDimension() {
    }

    public StatsLocationDimension(StatsCommonDimension statsCommonDimension, LocationDimension locationDimension) {
        this.statsCommonDimension = statsCommonDimension;
        this.locationDimension = locationDimension;
    }

    public StatsCommonDimension getStatsCommonDimension() {
        return statsCommonDimension;
    }

    public void setStatsCommonDimension(StatsCommonDimension statsCommonDimension) {
        this.statsCommonDimension = statsCommonDimension;
    }

    public LocationDimension getLocationDimension() {
        return locationDimension;
    }

    public void setLocationDimension(LocationDimension locationDimension) {
        this.locationDimension = locationDimension;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StatsLocationDimension)) return false;
        StatsLocationDimension that = (StatsLocationDimension) o;
        return Objects.equals(getStatsCommonDimension(), that.getStatsCommonDimension()) &&
                Objects.equals(getLocationDimension(), that.getLocationDimension());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getStatsCommonDimension(), getLocationDimension());
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (o == this) {
            return 0;
        }
        StatsLocationDimension other = (StatsLocationDimension) o;
        int tmp = this.statsCommonDimension.compareTo(other.statsCommonDimension);
        if (0 != tmp) {
            return tmp;
        }
        tmp = this.locationDimension.compareTo(other.locationDimension);
        return tmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.statsCommonDimension.write(out);
        this.locationDimension.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.statsCommonDimension.readFields(in);
        this.locationDimension.readFields(in);
    }

    /**
     * 克隆当前对象一个实例
     *
     * @param dimension
     * @return
     */
    public static StatsLocationDimension clone(StatsLocationDimension dimension) {
        StatsCommonDimension statsCommonDimension = StatsCommonDimension.clone(dimension.statsCommonDimension);
        LocationDimension locationDimension = new LocationDimension(
                dimension.locationDimension.getId(),
                dimension.locationDimension.getCountry(),
                dimension.locationDimension.getProvince(),
                dimension.locationDimension.getCity()
        );
        return new StatsLocationDimension(statsCommonDimension, locationDimension);
    }
}
