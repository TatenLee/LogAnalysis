package com.bigdata.util;

import com.bigdata.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @Author Taten
 * @Description 查看会员id是否是新增会员，过滤不合法的会员id
 **/
public class MemberUtil {
    private static Map<String, Boolean> cache = new LinkedHashMap<String, Boolean>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return this.size() > 1000;
        }
    };

    /**
     * 检测会员ID是否合法
     *
     * @param memeberId
     * @return true是合法，false不合法
     */
    public static boolean checkMemberId(String memeberId) {
        String regex = "^[0-9a-zA-Z].*$";
        if (StringUtils.isNotEmpty(memeberId)) {
            return memeberId.trim().matches(regex);
        }
        return false;
    }

    /**
     * 是否是一个新增的会员
     *
     * @param memberId
     * @param conn
     * @param conf
     * @return true是新增会员，false不是新增会员
     */
    public static boolean isNewMember(String memberId, Connection conn, Configuration conf) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        Boolean res = false;
        try {
            res = cache.get(memberId);
            if (null == res) {
                String sql = conf.get(GlobalConstants.PREFIX_TOTAL + "member_info");
                ps = conn.prepareStatement(sql);
                ps.setString(1, memberId);
                rs = ps.executeQuery();
                res = !rs.next();
                //添加到cache中
                cache.put(memberId, res);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null != res && res;
    }
}
