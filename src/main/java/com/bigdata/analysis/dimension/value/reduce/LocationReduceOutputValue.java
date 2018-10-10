package com.bigdata.analysis.dimension.value.reduce;

import com.bigdata.analysis.dimension.value.BaseOutputValue;
import com.bigdata.common.KpiType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author Taten
 * @Description 地域模块reduce阶段输出的value类型
 **/
public class LocationReduceOutputValue extends BaseOutputValue {
    private KpiType kpi;
    private int aus;
    private int session;
    private int bounce_sessions;

    @Override
    public KpiType getKpi() {
        return this.kpi;
    }

    @Override
    public void write(DataOutput out) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }
}
