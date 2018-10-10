package com.bigdata.analysis.mr.base;

import com.bigdata.analysis.dimension.BaseStatsDimension;
import com.bigdata.analysis.dimension.value.BaseOutputValue;
import com.bigdata.analysis.mr.service.IDimensionConvert;
import com.bigdata.analysis.mr.service.impl.IDimensionConvertImpl;
import com.bigdata.common.GlobalConstants;
import com.bigdata.common.KpiType;
import com.bigdata.util.JdbcUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author Taten
 * @Description 自定义reduce阶段输出格式类
 **/
public class IOutputWriterFormat extends OutputFormat<BaseStatsDimension, BaseOutputValue> {
    private static final Logger logger = Logger.getLogger(IOutputWriterFormat.class);

    @Override
    public RecordWriter<BaseStatsDimension, BaseOutputValue> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Configuration conf = taskAttemptContext.getConfiguration();
        Connection conn = JdbcUtil.getConn();
        IDimensionConvert convert = new IDimensionConvertImpl();
        return new IOutputRecordWriter(conn, conf, convert);
    }

    /**
     * 检测输出的空间
     *
     * @param jobContext
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
        // DO NOTHING
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new FileOutputCommitter(null, taskAttemptContext);
    }

    /**
     * 封装输出记录的内部类
     */
    public class IOutputRecordWriter extends RecordWriter<BaseStatsDimension, BaseOutputValue> {
        private Connection conn = null;
        private Configuration conf = null;
        private IDimensionConvert convert = null;

        // 定义两个集合用作缓存
        private Map<KpiType, Integer> batch = new HashMap<KpiType, Integer>();
        private Map<KpiType, PreparedStatement> map = new HashMap<KpiType, PreparedStatement>();

        public IOutputRecordWriter() {
        }

        public IOutputRecordWriter(Connection conn, Configuration conf, IDimensionConvert convert) {
            this.conn = conn;
            this.conf = conf;
            this.convert = convert;
        }

        @Override
        public void write(BaseStatsDimension key, BaseOutputValue value) throws IOException, InterruptedException {
            if (null == key || null == value) {
                return;
            }
            try {
                // 获取kpi
                KpiType kpi = value.getKpi();
                PreparedStatement ps = null;
                int counter = 1;
                if (map.containsKey(kpi)) {
                    ps = map.get(kpi);
                    counter = this.batch.get(kpi);
                    counter++;
                } else {
                    ps = conn.prepareStatement(conf.get(kpi.kpiName));
                    map.put(kpi, ps);
                }
                //将count添加到batch中
                this.batch.put(kpi, counter);

                //为ps赋值  writer_new_user
                String writerClassName = conf.get(GlobalConstants.PREFIX_WRITER + kpi.kpiName);
                Class<?> classz = Class.forName(writerClassName);
                IOutputWriter writer = (IOutputWriter) classz.newInstance(); // 将类转换成接口对象
                writer.writer(conf, ps, key, value, convert);  // 调用对应的实现类

                //将赋值好的ps达到一个批量就可以批量处理
                if (counter % GlobalConstants.NUM_OF_BATCH == 0) {
                    ps.executeBatch();
                    conn.commit();
                    this.batch.remove(kpi); // 移除已经执行的kpi的ps
                }
            } catch (Exception e) {
                logger.warn("Run failed of write method of recordWriter.", e);
            }
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            try {
                // 循环map并将其中的ps执行
                for (Map.Entry<KpiType, PreparedStatement> entry : map.entrySet()) {
                    entry.getValue().executeBatch();  // 将剩余的ps执行
                }
            } catch (SQLException e) {
                logger.error("Error in executing left prepared statement when closing.", e);
            } finally {
                JdbcUtil.close(conn, null, null);
                // 循环将执行完成后的ps进行移除
                for (Map.Entry<KpiType, PreparedStatement> entry : map.entrySet()) {
                    JdbcUtil.close(null, entry.getValue(), null);
                }
            }
        }
    }
}
