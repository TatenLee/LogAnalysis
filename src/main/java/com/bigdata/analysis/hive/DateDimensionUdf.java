package com.bigdata.analysis.hive;

import com.bigdata.analysis.dimension.base.DateDimension;
import com.bigdata.analysis.mr.service.IDimensionConvert;
import com.bigdata.analysis.mr.service.impl.IDimensionConvertImpl;
import com.bigdata.common.DateEnum;
import com.bigdata.util.TimeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @Author Taten
 * @Description 获取时间维度id
 **/
public class DateDimensionUdf extends UDF {
    private IDimensionConvert convert = new IDimensionConvertImpl();

    public int evaluate(String date) {
        if (StringUtils.isEmpty(date)) {
            date = TimeUtil.getYesterdayDate();
        }
        int id = -1;
        try {
            DateDimension dateDimension = DateDimension.buildDate(TimeUtil.parserString2Long(date), DateEnum.DAY);
            id = convert.getDimensionByValue(dateDimension);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return id;
    }
}
