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
 * @Description 地域维度类
 **/
public class LocationDimension extends BaseDimension {
    private int id;
    private String country;
    private String province;
    private String city;

    public LocationDimension() {
    }

    public LocationDimension(String country, String province, String city) {
        this.country = country;
        this.province = province;
        this.city = city;
    }

    public LocationDimension(int id, String country, String province, String city) {
        this(country, province, city);
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LocationDimension)) return false;
        LocationDimension that = (LocationDimension) o;
        return getId() == that.getId() &&
                Objects.equals(getCountry(), that.getCountry()) &&
                Objects.equals(getProvince(), that.getProvince()) &&
                Objects.equals(getCity(), that.getCity());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getCountry(), getProvince(), getCity());
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (o == this) {
            return 0;
        }
        LocationDimension other = (LocationDimension) o;
        int tmp = this.id - other.id;
        if (0 != tmp) {
            return tmp;
        }
        tmp = this.country.compareTo(other.country);
        if (0 != tmp) {
            return tmp;
        }
        tmp = this.province.compareTo(other.province);
        if (0 != tmp) {
            return tmp;
        }
        tmp = this.city.compareTo(other.city);
        return tmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeUTF(this.country);
        out.writeUTF(this.province);
        out.writeUTF(this.city);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.country = in.readUTF();
        this.province = in.readUTF();
        this.city = in.readUTF();
    }

    /**
     * 构建地域维度的集合对象
     *
     * @param country
     * @param province
     * @param city
     * @return
     */
    public static List<LocationDimension> buildList(String country, String province, String city) {
        if (StringUtils.isEmpty(country)) {
            country = province = city = GlobalConstants.DEFAULT_VALUE;
        }
        if (StringUtils.isEmpty(province)) {
            province = city = GlobalConstants.DEFAULT_VALUE;
        }
        if (StringUtils.isEmpty(city)) {
            city = GlobalConstants.DEFAULT_VALUE;
        }

        List<LocationDimension> list = new ArrayList<LocationDimension>();
        list.add(new LocationDimension(country, province, city));
        list.add(new LocationDimension(country, province, GlobalConstants.ALL_OF_VALUE));
        return list;
    }
}
