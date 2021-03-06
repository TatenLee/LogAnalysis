package com.bigdata.etl.tohbase;

import com.bigdata.common.EventLogConstants;
import com.bigdata.util.LogUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * @Auther: lyd
 * @Date: 2018/7/26 11:11
 * @Description:将解析后的数据存储到hbase中的mapper类
 */
public class LogToHbaseMapper extends Mapper<Object, Text, NullWritable, Put> {
    private static final Logger logger = Logger.getLogger(LogToHbaseMapper.class);
    private byte[] family = Bytes.toBytes(EventLogConstants.HBASE_COLUMN_FAMILY);
    //输入输出和过滤行记录
    private int inputRecords, outputRecords, filterRecords = 0;
    private CRC32 crc32 = new CRC32();


    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        this.inputRecords++;
        logger.info("输入的日志为:" + value.toString());
        Map<String, String> info = LogUtil.handleLog(value.toString());
        if (info.isEmpty()) {
            this.filterRecords++;
            return;
        }
        //获取事件
        String eventName = info.get(EventLogConstants.EVENT_COLUMN_NAME_EVENT_NAME);
        EventLogConstants.EventEnum event = EventLogConstants.EventEnum.valueOfAlias(eventName);
        switch (event) {
            case EVENT:
            case LAUNCH:
            case PAGEVIEW:
            case CHARGEREQUEST:
            case CHARGESUCCESS:
            case CHARGEREFUND:
                //将info存储
                handleInfo(info, eventName, context);
                break;
            default:
                filterRecords++;
                logger.warn("该事件暂时不支持数据的清洗.event:" + eventName);
                break;
        }
    }

    /**
     * 写数据到hbase中  根据日期获取hbase中某张表的该日期所在天的数据？？
     *
     * @param info
     * @param eventName
     * @param context
     */
    private void handleInfo(Map<String, String> info, String eventName, Context context) {
        try {
            if (!info.isEmpty()) {
                //取uuid s_time u_mid来构建row-key
                String uuid = info.get(EventLogConstants.EVENT_COLUMN_NAME_UUID);
                String serverTime = info.get(EventLogConstants.EVENT_COLUMN_NAME_SERVER_TIME);
                String memberId = info.get(EventLogConstants.EVENT_COLUMN_NAME_MEMBER_ID);
                //构建rowkey
                String rowKey = buildRowkey(uuid, serverTime, memberId, eventName);
                Put put = new Put(Bytes.toBytes(rowKey));
                for (Map.Entry<String, String> en : info.entrySet()) {
                    //将k-v添加到put
                    put.addColumn(family, Bytes.toBytes(en.getKey()), Bytes.toBytes(en.getValue()));
                }
                //输出
                context.write(NullWritable.get(), put);
                this.outputRecords++;
            }
        } catch (Exception e) {
            this.filterRecords++;
            logger.warn("写出到hbase异常.", e);
        }
    }

    /**
     * 构建row-key
     *
     * @param uuid
     * @param serverTime
     * @param memberId
     * @param eventName
     * @return
     */
    private String buildRowkey(String uuid, String serverTime, String memberId, String eventName) {
        StringBuffer sb = new StringBuffer();
        sb.append(serverTime + "_");
        //需要将crc32初始化
        crc32.reset();
        if (StringUtils.isNotEmpty(uuid)) {
            this.crc32.update(uuid.getBytes());
        }
        if (StringUtils.isNotEmpty(memberId)) {
            this.crc32.update(memberId.getBytes());
        }
        if (StringUtils.isNotEmpty(eventName)) {
            this.crc32.update(eventName.getBytes());
        }
        sb.append(this.crc32.getValue() % 1000000000L);
        return sb.toString();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        logger.info("输入、输出和过滤的记录数.inputRecords" + this.inputRecords
                + "  outputRecords:" + this.outputRecords + "  filterRecords:" + this.filterRecords);
    }
}
