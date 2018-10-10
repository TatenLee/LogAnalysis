package com.bigdata.analysis.dimension.value.map;

import com.bigdata.analysis.dimension.value.BaseOutputValue;
import com.bigdata.common.KpiType;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author Taten
 * @Description 地域模块map阶段输出的value类型
 **/
public class LocationMapOutputValue extends BaseOutputValue {
    private String uid;
    private String sid;

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    @Override
    public KpiType getKpi() {
        return null;

    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (StringUtils.isNotEmpty(uid)) {
            out.writeUTF(this.uid);
        } else {
            out.writeUTF("");
        }
        if (StringUtils.isNotEmpty(sid)) {
            out.writeUTF(this.sid);
        } else {
            out.writeUTF("");
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.uid = in.readUTF();
        this.sid = in.readUTF();
    }
}
