package com.bigdata.analysis.mr.nu;

import com.bigdata.analysis.dimension.StatsUserDimension;
import com.bigdata.analysis.dimension.value.map.TimeOutputValue;
import com.bigdata.analysis.dimension.value.reduce.TextOutputValue;
import com.bigdata.common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @Author Taten
 * @Description 新增用户的reducer类
 * 数据格式：
 * 2018-08-20 website 2000 new_user
 * 2018-08-20 all 3000 new_user
 * 2018-08-20 website 900 total_new_user
 **/
public class NewUserReducer extends Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension, TextOutputValue> {
    private static final Logger logger = Logger.getLogger(NewUserReducer.class);
    private StatsUserDimension k = new StatsUserDimension();
    private TextOutputValue v = new TextOutputValue();
    private Set<String> unique = new HashSet<String>();  // 用于uuid的去重统计

    /*
        2018-08-20 website (789,1237812947238)
        2018-08-20 ios (789,1237812947238)

        2018-08-20 all 788
        2018-08-20 all 789


        2018-08-20 website list((789,1237812947238))
        2018-08-20 ios list((789,1237812947238))

        2018-08-20 all list((788,1237812947238),(789,1237812947239))

        2018-08-20 website 1 mapwriteable(-1,1)
        2018-08-20 ios 1 mapwriteable(-1,1)
        2018-08-20 all 1  mapwriteable(-1,2)
     */
    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        // 清空set
        unique.clear();

        // 循环value，将uuid添加到set中，得到不重复的uuid个数
        for (TimeOutputValue value : values) {
            unique.add(value.getId());
        }

        // 构造输出的key
        k = key;

        // 构造输出的value
        // 设置kpi
        v.setKey(KpiType.valueOfKpiType(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
        // 将数据的个数放到mapWritable中
        MapWritable mapWritable = new MapWritable();
        mapWritable.put(new IntWritable(-1), new IntWritable(unique.size()));
        v.setValue(mapWritable);

        // 输出
        context.write(k, v);
    }
}
