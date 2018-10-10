package com.bigdata.analysis.mr.nm;

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
 * @Description 新增会员的reducer类
 **/
public class NewMemberReducer extends Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension, TextOutputValue> {
    private static final Logger logger = Logger.getLogger(NewMemberReducer.class);
    private StatsUserDimension k = new StatsUserDimension();
    private TextOutputValue v = new TextOutputValue();
    private Set<String> unique = new HashSet<String>();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        // 清空set
        unique.clear();

        // 循环value，将member id添加到set中，得到不重复的member id个数
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
