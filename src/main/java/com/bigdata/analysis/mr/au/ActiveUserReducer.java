package com.bigdata.analysis.mr.au;

import com.bigdata.analysis.dimension.StatsUserDimension;
import com.bigdata.analysis.dimension.value.map.TimeOutputValue;
import com.bigdata.analysis.dimension.value.reduce.TextOutputValue;
import com.bigdata.common.DateEnum;
import com.bigdata.common.KpiType;
import com.bigdata.util.TimeUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @Author Taten
 * @Description 活跃用户的reducer类
 **/
public class ActiveUserReducer extends Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension, TextOutputValue> {
    private static final Logger logger = Logger.getLogger(ActiveUserReducer.class);
    private StatsUserDimension k = new StatsUserDimension();
    private TextOutputValue v = new TextOutputValue();
    private Set<String> unique = new HashSet<String>();  // 用于uuid的去重统计
    private Map<Integer, Set<String>> hourlyMap = new HashMap<Integer, Set<String>>();
    private MapWritable hourlyWritable = new MapWritable();

    /**
     * 构建小时的容器
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        for (int i = 0; i < 24; i++) {
            hourlyMap.put(i, new HashSet<String>());
            hourlyWritable.put(new IntWritable(i), new IntWritable(0));
        }
    }

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        try {
            // 循环value，将uuid添加到set中，得到不重复的uuid个数
            for (TimeOutputValue value : values) {
                unique.add(value.getId());

                if (key.getStatsCommonDimension().getKpiDimension().getKpiName().equals(KpiType.ACTIVE_USER.kpiName)) {
                    //按小时的
                    int hour = TimeUtil.getDateInfo(value.getTime(), DateEnum.HOUR);
                    this.hourlyMap.get(hour).add(value.getId());
                }
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

            if (k.getStatsCommonDimension().getKpiDimension().getKpiName().equals(KpiType.ACTIVE_USER.kpiName)) {
                // 构建输出的value
                v.setKey(KpiType.HOURLY_ACTIVE_USER);

                // 循环
                for (Map.Entry<Integer, Set<String>> entry : this.hourlyMap.entrySet()) {
                    hourlyWritable.put(new IntWritable(entry.getKey()), new IntWritable(entry.getValue().size()));
                }
                v.setValue(hourlyWritable);

                // 输出
                context.write(k, v);
            }
        } finally {
            unique.clear();
            hourlyMap.clear();
            hourlyWritable.clear();
            for (int i = 0; i < 24; i++) {
                hourlyMap.put(i, new HashSet<String>());
                hourlyWritable.put(new IntWritable(i), new IntWritable(0));
            }
        }
    }
}
