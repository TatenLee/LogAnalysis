package com.bigdata.common;

/**
 * @Author Taten
 * @Description kpi的枚举
 **/
public enum KpiType {
    NEW_USER("new_user"),
    BROWSER_NEW_USER("browser_new_user"),
    ACTIVE_USER("active_user"),
    BROWSER_ACTIVE_USER("browser_active_user"),
    ACTIVE_MEMBER("active_member"),
    BROWSER_ACTIVE_MEMBER("browser_active_member"),
    NEW_MEMBER("new_member"),
    BROWSER_NEW_MEMBER("browser_new_member"),
    MEMBER_INFO("member_info"),
    SESSION("session"),
    BROWSER_SESSION("browser_session"),
    HOURLY_ACTIVE_USER("hourly_active_user"),
    HOURLY_SESSION("hourly_session"),
    PAGEVIEW("pageview"),
    LOCAL("local");

    public final String kpiName;

    KpiType(String kpiName) {
        this.kpiName = kpiName;
    }

    /**
     * 根据kpi的name获取kpi的枚举
     *
     * @param kpiName
     * @return
     */
    public static KpiType valueOfKpiType(String kpiName) {
        for (KpiType value : values()) {
            if (kpiName.equals(value.kpiName)) {
                return value;
            }
        }
        return null;
    }
}
