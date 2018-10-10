package com.bigdata.analysis.dimension.value.map;

import com.bigdata.analysis.dimension.value.BaseOutputValue;
import com.bigdata.common.KpiType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author Taten
 * @Description 用户模块和浏览器模块map阶段输出的value类型
 **/
public class TimeOutputValue extends BaseOutputValue {
    private String id;
    private long time;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    @Override
    public KpiType getKpi() {
        return null;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.id);
        out.writeLong(this.time);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.time = in.readLong();
    }
}
