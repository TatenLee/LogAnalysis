package com.bigdata.analysis.dimension.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @Author Taten
 * @Description 货币维度类
 **/
public class CurrencyDimension extends BaseDimension {
    private int id;
    private String currencyType;

    public CurrencyDimension() {
    }

    public CurrencyDimension(String currencyType) {
        this.currencyType = currencyType;
    }

    public CurrencyDimension(int id, String currencyType) {
        this(currencyType);
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCurrencyType() {
        return currencyType;
    }

    public void setCurrencyType(String currencyType) {
        this.currencyType = currencyType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CurrencyDimension)) return false;
        CurrencyDimension that = (CurrencyDimension) o;
        return getId() == that.getId() &&
                Objects.equals(getCurrencyType(), that.getCurrencyType());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getCurrencyType());
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }
        CurrencyDimension other = (CurrencyDimension) o;
        int tmp = this.id - other.id;
        if (0 != tmp) {
            return tmp;
        }
        tmp = this.currencyType.compareTo(other.currencyType);
        return tmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(this.id);
        out.writeUTF(this.currencyType);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.currencyType = in.readUTF();
    }
}
