package com.bigdata.analysis.dimension.value.reduce;

import com.bigdata.analysis.dimension.value.BaseOutputValue;
import com.bigdata.common.KpiType;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author Taten
 * @Description 用户模块和浏览器模块reduce阶段输出的value类型
 **/
public class TextOutputValue extends BaseOutputValue {
    private KpiType key;
    private MapWritable value = new MapWritable();

    public KpiType getKey() {
        return key;
    }

    public void setKey(KpiType key) {
        this.key = key;
    }

    public MapWritable getValue() {
        return value;
    }

    public void setValue(MapWritable value) {
        this.value = value;
    }

    @Override
    public KpiType getKpi() {
        return this.key;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeEnum(out, key);
        this.value.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        WritableUtils.readEnum(in, KpiType.class);
        this.value.readFields(in);
    }
}
