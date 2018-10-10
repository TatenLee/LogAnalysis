package com.bigdata.analysis.mr.service;

import com.bigdata.analysis.dimension.base.BaseDimension;

/**
 * @Author Taten
 * @Description 根据各个基础维度对象获取在数据库中对应的维度id
 **/
public interface IDimensionConvert {
    /**
     * 根据维度获取id
     *
     * @param dimension
     * @return
     */
    int getDimensionByValue(BaseDimension dimension);
}
