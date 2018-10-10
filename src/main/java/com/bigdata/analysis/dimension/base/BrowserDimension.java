package com.bigdata.analysis.dimension.base;

import com.bigdata.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @Author Taten
 * @Description 浏览器维度类
 **/
public class BrowserDimension extends BaseDimension {
    private int id;
    private String browserName;
    private String browserVersion;

    public BrowserDimension() {
    }

    public BrowserDimension(String browserName, String browserVersion) {
        this.browserName = browserName;
        this.browserVersion = browserVersion;
    }

    public BrowserDimension(int id, String browserName, String browserVersion) {
        this(browserName, browserVersion);
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getBrowserName() {
        return browserName;
    }

    public void setBrowserName(String browserName) {
        this.browserName = browserName;
    }

    public String getBrowserVersion() {
        return browserVersion;
    }

    public void setBrowserVersion(String browserVersion) {
        this.browserVersion = browserVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BrowserDimension)) return false;
        BrowserDimension that = (BrowserDimension) o;
        return getId() == that.getId() &&
                Objects.equals(getBrowserName(), that.getBrowserName()) &&
                Objects.equals(getBrowserVersion(), that.getBrowserVersion());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getBrowserName(), getBrowserVersion());
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (o == this) {
            return 0;
        }
        BrowserDimension other = (BrowserDimension) o;
        int tmp = this.id - other.id;
        if (0 != tmp) {
            return tmp;
        }
        tmp = this.browserName.compareTo(other.browserName);
        if (0 != tmp) {
            return tmp;
        }
        tmp = this.browserVersion.compareTo(other.browserVersion);
        return tmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeUTF(this.browserName);
        out.writeUTF(this.browserVersion);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.browserName = in.readUTF();
        this.browserVersion = in.readUTF();
    }

    /**
     * 获取当前类的实例
     *
     * @param browserName
     * @param browserVersion
     * @return
     */
    public static BrowserDimension newInstance(String browserName, String browserVersion) {
        BrowserDimension browserDimension = new BrowserDimension();
        browserDimension.browserName = browserName;
        browserDimension.browserVersion = browserVersion;
        return browserDimension;
    }

    /**
     * 构建浏览器维度的集合对象
     *
     * @param browserName
     * @param browserVersion
     * @return
     */
    public static List<BrowserDimension> buildList(String browserName, String browserVersion) {
        List<BrowserDimension> list = new ArrayList<BrowserDimension>();
        if (StringUtils.isEmpty(browserName)) {
            browserName = browserVersion = GlobalConstants.DEFAULT_VALUE;
        }

        if (StringUtils.isEmpty(browserVersion)) {
            browserVersion = GlobalConstants.DEFAULT_VALUE;
        }

        list.add(newInstance(browserName, browserVersion));
        list.add(newInstance(browserName, GlobalConstants.ALL_OF_VALUE));
        return list;
    }
}
