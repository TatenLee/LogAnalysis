package com.bigdata.analysis.mr.nu;

import com.bigdata.analysis.dimension.BaseStatsDimension;
import com.bigdata.analysis.dimension.StatsUserDimension;
import com.bigdata.analysis.dimension.value.BaseOutputValue;
import com.bigdata.analysis.dimension.value.reduce.TextOutputValue;
import com.bigdata.analysis.mr.base.IOutputWriter;
import com.bigdata.analysis.mr.service.IDimensionConvert;
import com.bigdata.common.GlobalConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author Taten
 * @Description 新增浏览器用户的ps赋值
 **/
public class NewBrowserUserWriter implements IOutputWriter {
    @Override
    public void writer(Configuration conf, PreparedStatement ps, BaseStatsDimension key, BaseOutputValue value, IDimensionConvert convert) throws IOException, SQLException {
        StatsUserDimension statsUserDimension = (StatsUserDimension) key;
        TextOutputValue textOutputValue = (TextOutputValue) value;
        int newUser = ((IntWritable)textOutputValue.getValue().get(new IntWritable(-1))).get();

        // 为ps赋值
        int i = 0;
        ps.setInt(++i, convert.getDimensionByValue(statsUserDimension.getStatsCommonDimension().getDateDimension()));
        ps.setInt(++i, convert.getDimensionByValue(statsUserDimension.getStatsCommonDimension().getPlatformDimension()));
        ps.setInt(++i, convert.getDimensionByValue(statsUserDimension.getBrowserDimension()));
        ps.setInt(++i, newUser);
        ps.setString(++i, conf.get(GlobalConstants.RUNNING_DATE));
        ps.setInt(++i, newUser);

        // 添加到批处理中
        ps.addBatch();
    }
}
