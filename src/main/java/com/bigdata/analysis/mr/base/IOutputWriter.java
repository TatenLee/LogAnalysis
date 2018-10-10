package com.bigdata.analysis.mr.base;

import com.bigdata.analysis.dimension.BaseStatsDimension;
import com.bigdata.analysis.dimension.value.BaseOutputValue;
import com.bigdata.analysis.mr.service.IDimensionConvert;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author Taten
 * @Description 为每一个指标的sql语句赋值的接口
 **/
public interface IOutputWriter {
    void writer(Configuration conf, PreparedStatement ps, BaseStatsDimension key, BaseOutputValue value, IDimensionConvert convert) throws IOException, SQLException;
}
