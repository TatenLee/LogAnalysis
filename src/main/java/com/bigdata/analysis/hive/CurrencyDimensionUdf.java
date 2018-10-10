package com.bigdata.analysis.hive;

import com.bigdata.analysis.dimension.base.CurrencyDimension;
import com.bigdata.analysis.mr.service.IDimensionConvert;
import com.bigdata.analysis.mr.service.impl.IDimensionConvertImpl;
import com.bigdata.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @Author Taten
 * @Description
 **/
public class CurrencyDimensionUdf extends UDF {
    public IDimensionConvert convert = null;

    public CurrencyDimensionUdf() {
        convert = new IDimensionConvertImpl();
    }

    public int evaluate(String name) {
        name = null == name || StringUtils.isEmpty(name.trim()) ? GlobalConstants.DEFAULT_VALUE : name.trim();
        CurrencyDimension currencyTypeDimension = new CurrencyDimension(name);
        try {
            return convert.getDimensionByValue(currencyTypeDimension);
        } catch (Exception e) {
            e.printStackTrace();
        }
        throw new RuntimeException("Exception in getting currency type dimension.");
    }
}
