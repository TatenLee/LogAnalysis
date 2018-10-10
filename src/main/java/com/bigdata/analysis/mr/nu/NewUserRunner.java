package com.bigdata.analysis.mr.nu;

import com.bigdata.analysis.dimension.StatsUserDimension;
import com.bigdata.analysis.dimension.base.DateDimension;
import com.bigdata.analysis.dimension.value.map.TimeOutputValue;
import com.bigdata.analysis.dimension.value.reduce.TextOutputValue;
import com.bigdata.analysis.mr.base.IOutputWriterFormat;
import com.bigdata.analysis.mr.service.IDimensionConvert;
import com.bigdata.analysis.mr.service.impl.IDimensionConvertImpl;
import com.bigdata.common.DateEnum;
import com.bigdata.common.EventLogConstants;
import com.bigdata.common.GlobalConstants;
import com.bigdata.util.JdbcUtil;
import com.bigdata.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author Taten
 * @Description 新增用户的驱动类
 **/
public class NewUserRunner implements Tool {
    private static final Logger logger = Logger.getLogger(NewUserRunner.class);
    private Configuration conf = new Configuration();

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new NewUserRunner(), args);
        } catch (Exception e) {
            logger.warn("Running failed of new user.", e);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.address", "hadoop01:8032");
        conf.setBoolean("mapreduce.app-submission.cross-platform", true);

        // 处理参数
        this.setArgs(conf, args);
        Job job = Job.getInstance(conf, "NewUser");
        job.setJarByClass(NewUserRunner.class);

        // 设置mapper属性
        TableMapReduceUtil.initTableMapperJob(this.buildList(job), NewUserMapper.class, StatsUserDimension.class, TimeOutputValue.class, job, true);
        job.setMapOutputKeyClass(StatsUserDimension.class);
        job.setMapOutputValueClass(TimeOutputValue.class);

        // 设置reducer属性
        job.setReducerClass(NewUserReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(TextOutputValue.class);

        // 设置输出类
        job.setOutputFormatClass(IOutputWriterFormat.class);

        if (job.waitForCompletion(true)) {
            this.computeTotalNewUser(job);  // 计算新增总用户
            return 0;
        } else {
            return 1;
        }
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
        filterList.addFilter(new SingleColumnValueFilter(
                Bytes.toBytes(EventLogConstants.HBASE_COLUMN_FAMILY),
                Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_EVENT_NAME),
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes(EventLogConstants.EventEnum.LAUNCH.alias)
        ));
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

    /**
     * 计算新增总用户
     * 获取运行当天的新增用户，再获取运行日期前一天的新增总用户，然后将相同维度的新增用户和总用户相加即可
     *
     * @param job
     */
    private void computeTotalNewUser(Job job) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            // 获取运行当天的日期
            long nowDate = TimeUtil.parserString2Long(job.getConfiguration().get(GlobalConstants.RUNNING_DATE));
            long yesterdayDate = nowDate - GlobalConstants.DAY_OF_MILISECONDS;

            // 构建时间维度对象
            DateDimension nowDateDimension = DateDimension.buildDate(nowDate, DateEnum.DAY);
            DateDimension yesterdayDimension = DateDimension.buildDate(yesterdayDate, DateEnum.DAY);

            // 获取对应时间维度的id
            int nowDimensionId = -1;
            int yesterdayDimensionId = -1;
            IDimensionConvert convert = new IDimensionConvertImpl();
            nowDimensionId = convert.getDimensionByValue(nowDateDimension);
            yesterdayDimensionId = convert.getDimensionByValue(yesterdayDimension);

            // 用户模块下
            // 获取昨天的新增总用户
            conn = JdbcUtil.getConn();
            Map<String, Integer> map = new HashMap<String, Integer>();
            if (yesterdayDimensionId > 0) {
                ps = conn.prepareStatement(conf.get(GlobalConstants.PREFIX_TOTAL + "new_total_user"));
                // 赋值
                ps.setInt(1, yesterdayDimensionId);
                rs = ps.executeQuery();
                while (rs.next()) {
                    int platformId = rs.getInt("platform_dimension_id");
                    int totalUser = rs.getInt("total_install_users");
                    // 存储
                    map.put(platformId + "", totalUser);
                }
            }

            //获取今天的新增用户
            if (nowDimensionId > 0) {
                ps = conn.prepareStatement(conf.get(GlobalConstants.PREFIX_TOTAL + "new_user"));
                //赋值
                ps.setInt(1, nowDimensionId);
                rs = ps.executeQuery();
                while (rs.next()) {
                    int platformId = rs.getInt("platform_dimension_id");
                    int newUsers = rs.getInt("new_install_users");
                    //存储
                    if (map.containsKey(platformId + "")) {
                        newUsers += map.get(platformId + "");
                    }
                    //覆盖
                    map.put(platformId + "", newUsers);
                }
            }

            //将map中数据进行更新
            ps = conn.prepareStatement(conf.get(GlobalConstants.PREFIX_TOTAL + "new_update_user"));
            for (Map.Entry<String, Integer> en : map.entrySet()) {
                //赋值
                ps.setInt(1, nowDimensionId);
                ps.setInt(2, Integer.parseInt(en.getKey()));
                ps.setInt(3, en.getValue());
                ps.setString(4, conf.get(GlobalConstants.RUNNING_DATE));
                ps.setInt(5, en.getValue());
                //执行
                ps.addBatch();
            }

            // 批量执行
            ps.executeBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            JdbcUtil.close(conn, ps, rs);
        }
    }
}
