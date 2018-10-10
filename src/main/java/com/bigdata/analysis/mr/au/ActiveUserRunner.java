package com.bigdata.analysis.mr.au;

import com.bigdata.analysis.dimension.StatsUserDimension;
import com.bigdata.analysis.dimension.value.map.TimeOutputValue;
import com.bigdata.analysis.dimension.value.reduce.TextOutputValue;
import com.bigdata.analysis.mr.base.IOutputWriterFormat;
import com.bigdata.common.EventLogConstants;
import com.bigdata.common.GlobalConstants;
import com.bigdata.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author Taten
 * @Description 活跃用户的驱动类
 **/
public class ActiveUserRunner implements Tool {
    private static final Logger logger = Logger.getLogger(ActiveUserRunner.class);
    private Configuration conf = new Configuration();

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new ActiveUserRunner(), args);
        } catch (Exception e) {
            logger.warn("Running failed of active user.", e);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        // 处理参数
        this.setArgs(conf, args);
        Job job = Job.getInstance(conf, "ActiveUser");
        job.setJarByClass(ActiveUserRunner.class);

        // 设置mapper属性
        TableMapReduceUtil.initTableMapperJob(this.buildList(job), ActiveUserMapper.class, StatsUserDimension.class, TimeOutputValue.class, job, true);
        job.setMapOutputKeyClass(StatsUserDimension.class);
        job.setMapOutputValueClass(TimeOutputValue.class);

        // 设置reducer属性
        job.setReducerClass(ActiveUserReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(TextOutputValue.class);

        // 设置输出类
        job.setOutputFormatClass(IOutputWriterFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    @Override
    public void setConf(Configuration conf) {
        conf.addResource("query-mapping.xml");
        conf.addResource("total-mapping.xml");
        conf.addResource("writer-mapping.xml");
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    /**
     * 处理输入的参数
     *
     * @param configuration
     * @param args
     */
    private void setArgs(Configuration configuration, String[] args) {
        String date = null;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-d") && i + 1 < args.length) {
                date = args[i + 1];
            }
        }
        // 如果时间为空或者是非法，都将默认运行昨天的数据
        if (StringUtils.isEmpty(date) || !TimeUtil.isValidateDate(date)) {
            date = TimeUtil.getYesterdayDate();
        }
        // 将时间存储到conf中
        configuration.set(GlobalConstants.RUNNING_DATE, date);
    }

    /**
     * 获取hbase的扫描对象
     *
     * @param job
     * @return
     */
    private List<Scan> buildList(Job job) {
        Configuration conf = job.getConfiguration();
        Long startDate = TimeUtil.parserString2Long(conf.get(GlobalConstants.RUNNING_DATE));
        Long endDate = startDate + GlobalConstants.DAY_OF_MILISECONDS;
        List<Scan> list = new ArrayList<Scan>();
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startDate + ""));
        scan.setStopRow(Bytes.toBytes(endDate + ""));

        // 过滤
        FilterList filterList = new FilterList();

        // 扫描哪些字段
        String[] columns = {
                EventLogConstants.EVENT_COLUMN_NAME_UUID,
                EventLogConstants.EVENT_COLUMN_NAME_SERVER_TIME,
                EventLogConstants.EVENT_COLUMN_NAME_PLATFORM,
                EventLogConstants.EVENT_COLUMN_NAME_BROWSER_NAME,
                EventLogConstants.EVENT_COLUMN_NAME_BROWSER_VERSION,
                EventLogConstants.EVENT_COLUMN_NAME_EVENT_NAME
        };
        // 将扫描的字段添加到过滤器中
        filterList.addFilter(this.getColumnFilter(columns));

        // 设置hbase表名
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(EventLogConstants.HBASE_TABLE_NAME));

        // 将过滤器添加到scan中
        scan.setFilter(filterList);

        // 将scan添加到list中
        list.add(scan);

        return list;
    }

    /**
     * 设置扫描的字段
     *
     * @param columns
     * @return
     */
    private Filter getColumnFilter(String[] columns) {
        int length = columns.length;
        byte[][] bytes = new byte[length][];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = Bytes.toBytes(columns[i]);
        }
        return new MultipleColumnPrefixFilter(bytes);
    }
}
