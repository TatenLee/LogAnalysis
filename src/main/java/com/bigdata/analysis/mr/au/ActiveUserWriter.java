package com.bigdata.analysis.mr.au;

import com.bigdata.analysis.dimension.BaseStatsDimension;
import com.bigdata.analysis.dimension.StatsUserDimension;
import com.bigdata.analysis.dimension.base.KpiDimension;
import com.bigdata.analysis.dimension.value.BaseOutputValue;
import com.bigdata.analysis.dimension.value.reduce.TextOutputValue;
import com.bigdata.analysis.mr.base.IOutputWriter;
import com.bigdata.analysis.mr.service.IDimensionConvert;
import com.bigdata.common.GlobalConstants;
import com.bigdata.common.KpiType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author Taten
 * @Description 新增用户的ps赋值
 **/
public class ActiveUserWriter implements IOutputWriter {
    private static final Logger log = Logger.getLogger(ActiveUserWriter.class);
    @Override
    public void writer(Configuration conf, PreparedStatement ps, BaseStatsDimension key, BaseOutputValue value, IDimensionConvert convert) throws IOException, SQLException {
        StatsUserDimension statsUserDimension = (StatsUserDimension) key;
        TextOutputValue textOutputValue = (TextOutputValue) value;



        int i = 0;
        switch (textOutputValue.getKpi()) {
            case ACTIVE_USER:
            case BROWSER_ACTIVE_USER:
                int activeUsers = ((IntWritable) textOutputValue.getValue().get(new IntWritable(-1))).get();
                //为ps赋值
                ps.setInt(++i, convert.getDimensionByValue(statsUserDimension.getStatsCommonDimension().getDateDimension()));
                ps.setInt(++i, convert.getDimensionByValue(statsUserDimension.getStatsCommonDimension().getPlatformDimension()));
                if (textOutputValue.getKpi().equals(KpiType.BROWSER_ACTIVE_USER)) {
                    ps.setInt(++i, convert.getDimensionByValue(statsUserDimension.getBrowserDimension()));
                }
                ps.setInt(++i, activeUsers);
                ps.setString(++i, conf.get(GlobalConstants.RUNNING_DATE));
                ps.setInt(++i, activeUsers);
                break;
            case HOURLY_ACTIVE_USER:
                ps.setInt(++i, convert.getDimensionByValue(statsUserDimension.getStatsCommonDimension().getDateDimension()));
                ps.setInt(++i, convert.getDimensionByValue(statsUserDimension.getStatsCommonDimension().getPlatformDimension()));
                ps.setInt(++i, convert.getDimensionByValue(new KpiDimension(KpiType.HOURLY_ACTIVE_USER.kpiName)));
                for (i++; i < 28; i++) {
                    int v = ((IntWritable) textOutputValue.getValue().get(new IntWritable(i - 4))).get();
                    ps.setInt(i, v);
                    ps.setInt(i + 25, v);
                }
                ps.setString(i, conf.get(GlobalConstants.RUNNING_DATE));
                System.out.println("===================");
                break;
        }

        //添加到批处理中
        ps.addBatch();
    }
}
