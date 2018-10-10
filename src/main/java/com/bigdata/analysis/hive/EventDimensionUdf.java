package com.bigdata.analysis.hive;

import com.bigdata.analysis.dimension.base.EventDimension;
import com.bigdata.analysis.mr.service.IDimensionConvert;
import com.bigdata.analysis.mr.service.impl.IDimensionConvertImpl;
import com.bigdata.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @Author Taten
 * @Description 获取事件维度id
 **/
public class EventDimensionUdf extends UDF {
    private IDimensionConvert convert = new IDimensionConvertImpl();

    public int evaluate(String category, String action) {
        if (StringUtils.isEmpty(category)) {
            category = action = GlobalConstants.DEFAULT_VALUE;
        }
        if (StringUtils.isEmpty(action)) {
            action = GlobalConstants.DEFAULT_VALUE;
        }
        int id = -1;
        try {
            EventDimension eventDimension = new EventDimension(category, action);
            id = convert.getDimensionByValue(eventDimension);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return id;
    }
}
