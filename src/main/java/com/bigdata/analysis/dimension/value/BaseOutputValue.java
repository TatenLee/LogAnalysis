package com.bigdata.analysis.dimension.value;

import com.bigdata.common.KpiType;
import org.apache.hadoop.io.Writable;

/**
 * @Author Taten
 * @Description map或者是reduce阶段输出的value类型的顶级父类
 **/
public abstract class BaseOutputValue implements Writable {
    public abstract KpiType getKpi();
}
