package com.bigdata.analysis.hive;

import com.bigdata.analysis.dimension.base.PaymentTypeDimension;
import com.bigdata.analysis.mr.service.IDimensionConvert;
import com.bigdata.analysis.mr.service.impl.IDimensionConvertImpl;
import com.bigdata.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @Author Taten
 * @Description 获取支付方式维度的Id
 **/
public class PaymentTypeDimensionUdf extends UDF {
    public IDimensionConvert convert = null;

    public PaymentTypeDimensionUdf() {
        convert = new IDimensionConvertImpl();
    }

    public int evaluate(String name) {
        name = null == name || StringUtils.isEmpty(name.trim()) ? GlobalConstants.DEFAULT_VALUE : name.trim();
        PaymentTypeDimension paymentTypeDimension = new PaymentTypeDimension(name);
        try {
            return convert.getDimensionByValue(paymentTypeDimension);
        } catch (Exception e) {
            e.printStackTrace();
        }
        throw new RuntimeException("Exception in getting payment type dimension.");
    }
}
