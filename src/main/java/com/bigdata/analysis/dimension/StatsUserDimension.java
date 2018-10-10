package com.bigdata.analysis.dimension;

import com.bigdata.analysis.dimension.base.BaseDimension;
import com.bigdata.analysis.dimension.base.BrowserDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @Author Taten
 * @Description 封装用户模块和浏览器模块中map和reduce阶段输出的key的类型
 **/
public class StatsUserDimension extends BaseStatsDimension {
    private StatsCommonDimension statsCommonDimension = new StatsCommonDimension();
    private BrowserDimension browserDimension = new BrowserDimension();

    public StatsUserDimension() {
    }

    public StatsUserDimension(StatsCommonDimension statsCommonDimension, BrowserDimension browserDimension) {
        this.statsCommonDimension = statsCommonDimension;
        this.browserDimension = browserDimension;
    }

    public StatsCommonDimension getStatsCommonDimension() {
        return statsCommonDimension;
    }

    public void setStatsCommonDimension(StatsCommonDimension statsCommonDimension) {
        this.statsCommonDimension = statsCommonDimension;
    }

    public BrowserDimension getBrowserDimension() {
        return browserDimension;
    }

    public void setBrowserDimension(BrowserDimension browserDimension) {
        this.browserDimension = browserDimension;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StatsUserDimension)) return false;
        StatsUserDimension that = (StatsUserDimension) o;
        return Objects.equals(getStatsCommonDimension(), that.getStatsCommonDimension()) &&
                Objects.equals(getBrowserDimension(), that.getBrowserDimension());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getStatsCommonDimension(), getBrowserDimension());
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (o == this) {
            return 0;
        }
        StatsUserDimension other = (StatsUserDimension) o;
        int tmp = this.statsCommonDimension.compareTo(other.statsCommonDimension);
        if (0 != tmp) {
            return tmp;
        }
        tmp = this.browserDimension.compareTo(other.browserDimension);
        return tmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.statsCommonDimension.write(out);
        this.browserDimension.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.statsCommonDimension.readFields(in);
        this.browserDimension.readFields(in);
    }

    /**
     * 克隆当前对象一个实例
     *
     * @param dimension
     * @return
     */
    public static StatsUserDimension clone(StatsUserDimension dimension) {
        StatsCommonDimension statsCommonDimension = StatsCommonDimension.clone(dimension.statsCommonDimension);
        BrowserDimension browserDimension = new BrowserDimension(
                dimension.browserDimension.getId(),
                dimension.browserDimension.getBrowserName(),
                dimension.browserDimension.getBrowserVersion()
        );
        return new StatsUserDimension(statsCommonDimension, browserDimension);
    }
}
