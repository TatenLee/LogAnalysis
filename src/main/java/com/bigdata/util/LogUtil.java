package com.bigdata.util;

import com.bigdata.common.EventLogConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Auther: lyd
 * @Date: 2018/8/17 09:41
 * @Description:正行日志的解析工具。
 */
public class LogUtil {
    private static final Logger logger = Logger.getLogger(LogUtil.class);


    /**
     * 单行日志的解析
     *
     * @param log 192.168.216.1^A1534231880.767^A192.168.216.111^A/qf.png?uuid=1289-dfsf-erw2&name=zs
     * @return
     */
    public static Map<String, String> handleLog(String log) {
        Map<String, String> info = new ConcurrentHashMap<String, String>();
        if (StringUtils.isNotEmpty(log.trim())) {
            //拆分单行日志
            String[] fields = log.split(EventLogConstants.COLUMN_SEPARTOR);
            if (fields.length == 4) {
                //存储数据到info中
                info.put(EventLogConstants.EVENT_COLUMN_NAME_IP, fields[0]);
                info.put(EventLogConstants.EVENT_COLUMN_NAME_SERVER_TIME, fields[1].replaceAll("\\.", ""));
                //处理参数列表
                handleParams(info, fields[3]);
                //在处理ip
                handleIp(info);
                //处理useragent
                handleUserAgent(info);
            }
        }
        return info;
    }

    /**
     * 处理agent
     *
     * @param info
     */
    private static void handleUserAgent(Map<String, String> info) {
        if (info.containsKey(EventLogConstants.EVENT_COLUMN_NAME_USERAGENT)) {
            UserAgentUtil.UserAgentInfo ua = UserAgentUtil.parseUserAgent(info.get(EventLogConstants.EVENT_COLUMN_NAME_USERAGENT));
            if (ua != null) {
                info.put(EventLogConstants.EVENT_COLUMN_NAME_BROWSER_NAME, ua.getBrowserName());
                info.put(EventLogConstants.EVENT_COLUMN_NAME_BROWSER_VERSION, ua.getBrowserVersion());
                info.put(EventLogConstants.EVENT_COLUMN_NAME_OS_NAME, ua.getOsName());
                info.put(EventLogConstants.EVENT_COLUMN_NAME_OS_VERSION, ua.getOsVersion());
            }
        }
    }

    /**
     * 处理ip
     *
     * @param info
     */
    private static void handleIp(Map<String, String> info) {
        if (info.containsKey(EventLogConstants.EVENT_COLUMN_NAME_IP)) {
            IPParserUtil.RegionInfo ri = new IPParserUtil().parserIp(info.get(EventLogConstants.EVENT_COLUMN_NAME_IP));
            if (ri != null) {
                info.put(EventLogConstants.EVENT_COLUMN_NAME_COUNTRY, ri.getCountry());
                info.put(EventLogConstants.EVENT_COLUMN_NAME_PROVINCE, ri.getProvince());
                info.put(EventLogConstants.EVENT_COLUMN_NAME_CITY, ri.getCity());
            }
        }
    }

    /**
     * 处理参数
     *
     * @param info
     * @param field
     */
    private static void handleParams(Map<String, String> info, String field) {
        if (StringUtils.isNotEmpty(field)) {
            int index = field.indexOf("?");
            if (index > 0) {
                String fields = field.substring(index + 1);
                String[] params = fields.split("&");
                for (int i = 0; i < params.length; i++) {
                    String[] kvs = params[i].split("=");
                    try {
                        String k = kvs[0];
                        String v = URLDecoder.decode(kvs[1], "utf-8");
                        if (StringUtils.isNotEmpty(k)) {
                            //存储数据到info
                            info.put(k, v);
                        }
                    } catch (UnsupportedEncodingException e) {
                        logger.warn("url的解码异常.", e);
                    }
                }
            }
        }
    }
}
