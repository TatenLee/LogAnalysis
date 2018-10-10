package com.bigdata.util;

import com.bigdata.common.GlobalConstants;

import java.sql.*;

/**
 * @Author Taten
 * @Description 获取mysql的连接和关闭
 **/
public class JdbcUtil {
    static {
        try {
            Class.forName(GlobalConstants.DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取mysql的连接
     *
     * @return
     */
    public static Connection getConn() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(GlobalConstants.URL, GlobalConstants.USERNAME, GlobalConstants.PASSWORD);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 关闭相关对象
     *
     * @param conn
     * @param ps
     * @param rs
     */
    public static void close(Connection conn, PreparedStatement ps, ResultSet rs) {
        if (null != conn) {
            try {
                conn.close();
            } catch (SQLException e) {
                // DO NOTHING
            }
        }

        if (null != ps) {
            try {
                ps.close();
            } catch (SQLException e) {
                // DO NOTHING
            }
        }

        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException e) {
                // DO NOTHING
            }
        }
    }
}
