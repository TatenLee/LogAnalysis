package com.bigdata.analysis.mr.service.impl;

import com.bigdata.analysis.dimension.base.*;
import com.bigdata.analysis.mr.service.IDimensionConvert;
import com.bigdata.util.JdbcUtil;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @Author Taten
 * @Description 根据维度获取维度id的接口实现
 **/
public class IDimensionConvertImpl implements IDimensionConvert {
    private static final Logger logger = Logger.getLogger(IDimensionConvertImpl.class);
    private Map<String, Integer> cache = new LinkedHashMap<String, Integer>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
            return this.size() > 1000;
        }
    };

    /**
     * 获取对应维度的id
     *
     * @param dimension
     * @return
     */
    @Override
    public int getDimensionByValue(BaseDimension dimension) {
        try {
            //生成维度缓存key
            String cacheKey = buildCacheKey(dimension);
            if (this.cache.containsKey(cacheKey)) {
                return this.cache.get(cacheKey);
            }
            //代码走到这，代表cache中没有对应维度
            //去mysql中先查找，如有则返回id，如没有将先插入再返回维度id
            Connection conn = JdbcUtil.getConn();
            String[] sqls = null;
            if (dimension instanceof PlatformDimension) {
                sqls = buildPlatformSqls(dimension);
            } else if (dimension instanceof KpiDimension) {
                sqls = buildKpiSqls(dimension);
            } else if (dimension instanceof BrowserDimension) {
                sqls = buildBrowserSqls(dimension);
            } else if (dimension instanceof DateDimension) {
                sqls = buildDateSqls(dimension);
            } else if (dimension instanceof LocationDimension) {
                sqls = buildLocalSqls(dimension);
            } else if (dimension instanceof EventDimension) {
                sqls = buildEventSqls(dimension);
            } else if (dimension instanceof CurrencyDimension) {
                sqls = this.buildCurrencySqls(dimension);
            } else if (dimension instanceof PaymentTypeDimension) {
                sqls = this.buildPaymentSqls(dimension);
            }

            //执行sql
            int id = -1;
            synchronized (this) {
                id = this.executeSqls(conn, dimension, sqls);
            }
            //将获取的id放到缓存汇总
            this.cache.put(cacheKey, id);
            return id;
        } catch (Exception e) {
            logger.warn("Exception in getting dimension id.", e);
        }
        throw new RuntimeException("Exception in getting dimension id");
    }

    /**
     * 构建缓存key,
     *
     * @param dimension
     * @return
     */
    private String buildCacheKey(BaseDimension dimension) {
        StringBuffer sb = new StringBuffer();
        if (dimension instanceof PlatformDimension) {
            PlatformDimension platform = (PlatformDimension) dimension;
            sb.append("platform_");
            sb.append(platform.getPlatformName());
        } else if (dimension instanceof KpiDimension) {
            KpiDimension kpi = (KpiDimension) dimension;
            sb.append("kpi_");
            sb.append(kpi.getKpiName());
        } else if (dimension instanceof BrowserDimension) {
            BrowserDimension browser = (BrowserDimension) dimension;
            sb.append("browser_");
            sb.append(browser.getBrowserName());
            sb.append(browser.getBrowserVersion());
        } else if (dimension instanceof DateDimension) {
            DateDimension date = (DateDimension) dimension;
            sb.append("date_");
            sb.append(date.getYear());
            sb.append(date.getSeason());
            sb.append(date.getMonth());
            sb.append(date.getWeek());
            sb.append(date.getDay());
            sb.append(date.getType());
        } else if (dimension instanceof LocationDimension) {
            LocationDimension local = (LocationDimension) dimension;
            sb.append("local_");
            sb.append(local.getCountry());
            sb.append(local.getProvince());
            sb.append(local.getCity());
        } else if (dimension instanceof EventDimension) {
            EventDimension event = (EventDimension) dimension;
            sb.append("event_");
            sb.append(event.getCategory());
            sb.append(event.getAction());
        } else if (dimension instanceof CurrencyDimension) {
            sb.append("currency_");
            CurrencyDimension currency = (CurrencyDimension) dimension;
            sb.append(currency.getCurrencyType());
        } else if (dimension instanceof PaymentTypeDimension) {
            sb.append("payment_");
            PaymentTypeDimension payment = (PaymentTypeDimension) dimension;
            sb.append(payment.getPaymentType());
        }
        return 0 == sb.length() ? null : sb.toString();
    }

    /**
     * 第一个查询id的sql，第二个插入sql
     *
     * @param dimension
     * @return
     */
    private String[] buildPlatformSqls(BaseDimension dimension) {
        String query = "select id from `dimension_platform` where `platform_name` = ?";
        String insert = "insert into `dimension_platform`(`platform_name`) values(?)";
        return new String[]{query, insert};
    }

    private String[] buildBrowserSqls(BaseDimension dimension) {
        String query = "select id from `dimension_browser` where `browser_name` = ? and `browser_version` = ?";
        String insert = "insert into `dimension_browser`(`browser_name`,`browser_version`) values(?,?)";
        return new String[]{query, insert};
    }


    private String[] buildKpiSqls(BaseDimension dimension) {
        String query = "select id from `dimension_kpi` where `kpi_name` = ?";
        String insert = "insert into `dimension_kpi`(`kpi_name`) values(?)";
        return new String[]{query, insert};
    }

    private String[] buildDateSqls(BaseDimension dimension) {
        String query = "select id from `dimension_date` where `year` = ? and `season` = ? and `month` = ? and `week` = ? and `day` = ? and `type` = ? and `calendar` = ? ";
        String insert = "insert into `dimension_date`(`year` , `season` , `month` , `week` , `day` , `type` , `calendar`) values(?,?,?,?,?,?,?)";
        return new String[]{query, insert};
    }

    private String[] buildLocalSqls(BaseDimension dimension) {
        String query = "select id from `dimension_location` where `country` = ? and `province` = ? and `city` = ? ";
        String insert = "insert into `dimension_location`(`country` , `province` , `city`) values(?,?,?)";
        return new String[]{query, insert};
    }

    private String[] buildEventSqls(BaseDimension dimension) {
        String query = "select id from `dimension_event` where `category` = ? and `action` = ? ";
        String insert = "insert into `dimension_event`(`category` , `action` ) values(?,?)";
        return new String[]{query, insert};
    }

    private String[] buildCurrencySqls(BaseDimension dimension) {
        String query = "select id from `dimension_currency_type` where `currency_name` = ?";
        String insert = "insert into `dimension_currency_type`(`currency_name`) values(?)";
        return new String[]{query, insert};
    }

    private String[] buildPaymentSqls(BaseDimension dimension) {
        String query = "select id from `dimension_payment_type` where `payment_type` = ?";
        String insert = "insert into `dimension_payment_type`(`payment_type`) values(?)";
        return new String[]{query, insert};
    }

    /**
     * 执行sql
     *
     * @param sqls
     * @param dimension
     * @param conn
     * @return
     */
    private int executeSqls(Connection conn, BaseDimension dimension, String[] sqls) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            // 先查询
            ps = conn.prepareStatement(sqls[0]);
            // 赋值
            this.setArgs(dimension, ps);
            rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            }
            // 代码走到这儿，代表没有查询到对应的id，则插入并查询
            ps = conn.prepareStatement(sqls[1], Statement.RETURN_GENERATED_KEYS);
            this.setArgs(dimension, ps);
            ps.executeUpdate();
            rs = ps.getGeneratedKeys();
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            logger.warn("Exception in executing sql.", e);
        }
        throw new RuntimeException("Exception in executing sql");
    }

    /**
     * 设置参数
     *
     * @param ps
     * @param dimension
     */
    private void setArgs(BaseDimension dimension, PreparedStatement ps) {
        try {
            int i = 0;
            if (dimension instanceof PlatformDimension) {
                PlatformDimension platform = (PlatformDimension) dimension;
                ps.setString(++i, platform.getPlatformName());
            } else if (dimension instanceof KpiDimension) {
                KpiDimension kpi = (KpiDimension) dimension;
                ps.setString(++i, kpi.getKpiName());
            } else if (dimension instanceof BrowserDimension) {
                BrowserDimension browser = (BrowserDimension) dimension;
                ps.setString(++i, browser.getBrowserName());
                ps.setString(++i, browser.getBrowserVersion());
            } else if (dimension instanceof DateDimension) {
                DateDimension date = (DateDimension) dimension;
                ps.setInt(++i, date.getYear());
                ps.setInt(++i, date.getSeason());
                ps.setInt(++i, date.getMonth());
                ps.setInt(++i, date.getWeek());
                ps.setInt(++i, date.getDay());
                ps.setString(++i, date.getType());
                ps.setDate(++i, new Date(date.getCalendar().getTime()));
            } else if (dimension instanceof LocationDimension) {
                LocationDimension local = (LocationDimension) dimension;
                ps.setString(++i, local.getCountry());
                ps.setString(++i, local.getProvince());
                ps.setString(++i, local.getCity());
            } else if (dimension instanceof EventDimension) {
                EventDimension event = (EventDimension) dimension;
                ps.setString(++i, event.getCategory());
                ps.setString(++i, event.getAction());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
