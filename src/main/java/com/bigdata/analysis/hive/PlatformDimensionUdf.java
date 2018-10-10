package com.bigdata.analysis.hive;

import com.bigdata.analysis.dimension.base.PlatformDimension;
import com.bigdata.analysis.mr.service.IDimensionConvert;
import com.bigdata.analysis.mr.service.impl.IDimensionConvertImpl;
import com.bigdata.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @Author Taten
 * @Description 获取平台维度id
 **/
public class PlatformDimensionUdf extends UDF {
    private IDimensionConvert convert = new IDimensionConvertImpl();

    public int evaluate(String platform) {
        if (StringUtils.isEmpty(platform)) {
            platform = GlobalConstants.DEFAULT_VALUE;
        }
        int id = -1;
        try {
            PlatformDimension platformDimension = new PlatformDimension(platform);
            id = convert.getDimensionByValue(platformDimension);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return id;
    }
}
