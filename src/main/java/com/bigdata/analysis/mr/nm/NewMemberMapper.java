package com.bigdata.analysis.mr.nm;

import com.bigdata.analysis.dimension.StatsUserDimension;
import com.bigdata.analysis.dimension.base.BrowserDimension;
import com.bigdata.analysis.dimension.base.DateDimension;
import com.bigdata.analysis.dimension.base.PlatformDimension;
import com.bigdata.analysis.dimension.value.map.TimeOutputValue;
import com.bigdata.common.DateEnum;
import com.bigdata.common.EventLogConstants;
import com.bigdata.common.KpiType;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * @Author Taten
 * @Description 统计新增的会员，launch时间中member id的去重个数
 **/
public class NewMemberMapper extends TableMapper<StatsUserDimension, TimeOutputValue> {
    private static final Logger logger = Logger.getLogger(NewMemberMapper.class);
    private StatsUserDimension k = new StatsUserDimension();
    private TimeOutputValue v = new TimeOutputValue();
    private byte[] family = Bytes.toBytes(EventLogConstants.HBASE_COLUMN_FAMILY);

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        // 从hbase中获取数据
        String memberId = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_MEMBER_ID)));
        String serverTime = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_SERVER_TIME)));
        String platformName = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_PLATFORM)));
        String browserName = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_BROWSER_NAME)));
        String browserVersion = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.EVENT_COLUMN_NAME_BROWSER_VERSION)));

        if (StringUtils.isEmpty(memberId) || StringUtils.isEmpty(serverTime)) {
            logger.warn("member id and server time must not be null. member id: " + memberId + " serverTime: " + serverTime);
        }

        // 构建输出的value
        v.setId(memberId);
        v.setTime(Long.valueOf(serverTime));

        // 构建输出的key
        k.getBrowserDimension().setBrowserName("");
        k.getBrowserDimension().setBrowserVersion("");
        k.getStatsCommonDimension().setDateDimension(DateDimension.buildDate(Long.valueOf(serverTime), DateEnum.DAY));
        k.getStatsCommonDimension().getKpiDimension().setKpiName(KpiType.NEW_MEMBER.kpiName);

        // 循环平台维度输出
        List<PlatformDimension> platformDimensionList = PlatformDimension.buildList(platformName);
        for (PlatformDimension platformDimension : platformDimensionList) {
            k.getStatsCommonDimension().setPlatformDimension(platformDimension);
            context.write(k, v);

            // 循环浏览器维度输出
            k.getStatsCommonDimension().getKpiDimension().setKpiName(KpiType.BROWSER_NEW_MEMBER.kpiName);
            List<BrowserDimension> browserDimensionList = BrowserDimension.buildList(browserName, browserVersion);
            for (BrowserDimension browserDimension : browserDimensionList) {
                k.setBrowserDimension(browserDimension);
                context.write(k, v);
            }
        }
    }
}
