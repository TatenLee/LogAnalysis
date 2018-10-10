package com.bigdata.common;

/**
 * @Author Taten
 * @Description 全局的常量类
 **/
public class GlobalConstants {
    public static final String ALL_OF_VALUE = "all";

    public static final String DEFAULT_VALUE = "unknown";

    public static final String PREFIX_OUTPUT = "output_";

    public static final String PREFIX_TOTAL = "total_";

    public static final String PREFIX_WRITER = "writer_";

    public static final String RUNNING_DATE = "running_date";

    public static final String USERNAME = "root";

    public static final String PASSWORD = "mysql";

    public static final String DRIVER = "com.mysql.cj.jdbc.Driver";

    public static final String URL = "jdbc:mysql://hadoop01:3306/logs?createDatabaseIfNotExist=true&useSSL=false";

    public static final int NUM_OF_BATCH = 50;

    public static final long DAY_OF_MILISECONDS = 86400000L;//24*60*60*1000
}
