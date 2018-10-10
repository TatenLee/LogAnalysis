package com.bigdata.analysis.dimension.base;

import com.bigdata.common.DateEnum;
import com.bigdata.util.TimeUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.Objects;

/**
 * @Author Taten
 * @Description 时间维度类
 **/
public class DateDimension extends BaseDimension {
    private int id;
    private int year;
    private int season;
    private int month;
    private int week;
    private int day;
    private Date calendar = new Date();
    private String type;  // 指标的类型，如天指标，月指标

    public DateDimension() {
    }

    public DateDimension(int year, int season, int month, int week, int day) {
        this.year = year;
        this.season = season;
        this.month = month;
        this.week = week;
        this.day = day;
    }

    public DateDimension(int year, int season, int month, int week, int day, Date calendar) {
        this(year, season, month, week, day);
        this.calendar = calendar;
    }

    public DateDimension(int year, int season, int month, int week, int day, Date calendar, String type) {
        this(year, season, month, week, day, calendar);
        this.type = type;
    }

    public DateDimension(int id, int year, int season, int month, int week, int day, Date calendar, String type) {
        this(year, season, month, week, day, calendar, type);
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getSeason() {
        return season;
    }

    public void setSeason(int season) {
        this.season = season;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getWeek() {
        return week;
    }

    public void setWeek(int week) {
        this.week = week;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public Date getCalendar() {
        return calendar;
    }

    public void setCalendar(Date calendar) {
        this.calendar = calendar;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DateDimension)) return false;
        DateDimension that = (DateDimension) o;
        return getId() == that.getId() &&
                getYear() == that.getYear() &&
                getSeason() == that.getSeason() &&
                getMonth() == that.getMonth() &&
                getWeek() == that.getWeek() &&
                getDay() == that.getDay() &&
                Objects.equals(getCalendar(), that.getCalendar()) &&
                Objects.equals(getType(), that.getType());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getYear(), getSeason(), getMonth(), getWeek(), getDay(), getCalendar(), getType());
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (o == this) {
            return 0;
        }
        DateDimension other = (DateDimension) o;
        int tmp = this.id - other.id;
        if (0 != tmp) {
            return tmp;
        }
        tmp = this.year - other.year;
        if (0 != tmp) {
            return tmp;
        }
        tmp = this.season - other.season;
        if (0 != tmp) {
            return tmp;
        }
        tmp = this.month - other.month;
        if (0 != tmp) {
            return tmp;
        }
        tmp = this.week - other.week;
        if (0 != tmp) {
            return tmp;
        }
        tmp = this.day - other.day;
        if (0 != tmp) {
            return tmp;
        }
        tmp = this.type.compareTo(other.type);
        return tmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeInt(this.year);
        out.writeInt(this.season);
        out.writeInt(this.month);
        out.writeInt(this.week);
        out.writeInt(this.day);
        out.writeLong(this.calendar.getTime());  //date类型写成long
        out.writeUTF(this.type);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.year = in.readInt();
        this.season = in.readInt();
        this.month = in.readInt();
        this.week = in.readInt();
        this.day = in.readInt();
        this.calendar.setTime(in.readLong()); //date类型的读
        this.type = in.readUTF();
    }

    /**
     * 根据时间戳和type获取时间的维度
     *
     * @param time
     * @param dateType
     * @return
     */
    public static DateDimension buildDate(long time, DateEnum dateType) {
        int year = TimeUtil.getDateInfo(time, DateEnum.YEAR);
        Calendar calendar = Calendar.getInstance();
        calendar.clear();  //先先清除日历对象
        //判断type的类型
        if (DateEnum.YEAR.equals(dateType)) {  //当年的1月1号这一天
            calendar.set(year, 0, 1);
            return new DateDimension(year, 1, 0, 0, 1, calendar.getTime(), dateType.dateType);
        }
        int season = TimeUtil.getDateInfo(time, DateEnum.SEASON);
        if (DateEnum.SEASON.equals(dateType)) {  //当季度的第一个月的1号这一天
            int month = season * 3 - 2;
            calendar.set(year, month - 1, 1);
            return new DateDimension(year, season, month, 0, 1, calendar.getTime(), dateType.dateType);
        }
        int month = TimeUtil.getDateInfo(time, DateEnum.MONTH);
        if (DateEnum.MONTH.equals(dateType)) {  //当月1号这一天
            calendar.set(year, month - 1, 1);
            return new DateDimension(year, season, month, 0, 1, calendar.getTime(), dateType.dateType);
        }
        int week = TimeUtil.getDateInfo(time, DateEnum.WEEK);
        if (DateEnum.WEEK.equals(dateType)) {  //当周的第一天的0时0分0秒
            long firstDayOfWeek = TimeUtil.getFirstDayOfWeek(time);
            year = TimeUtil.getDateInfo(firstDayOfWeek, DateEnum.YEAR);
            season = TimeUtil.getDateInfo(firstDayOfWeek, DateEnum.SEASON);
            month = TimeUtil.getDateInfo(firstDayOfWeek, DateEnum.MONTH);
            week = TimeUtil.getDateInfo(firstDayOfWeek, DateEnum.WEEK);
            int day = TimeUtil.getDateInfo(firstDayOfWeek, DateEnum.DAY);
            calendar.set(year, month - 1, 1);
            return new DateDimension(year, season, month, week, day, calendar.getTime(), dateType.dateType);
        }
        int day = TimeUtil.getDateInfo(time, DateEnum.DAY);
        if (DateEnum.DAY.equals(dateType)) {  //当月1号这一天
            calendar.set(year, month - 1, day);
            return new DateDimension(year, season, month, week, day, calendar.getTime(), dateType.dateType);
        }
        throw new RuntimeException("no support for the date type. dateType: " + dateType.dateType);
    }
}
