package analysis;

import com.bigdata.analysis.dimension.base.PlatformDimension;
import com.bigdata.analysis.mr.service.IDimensionConvert;
import com.bigdata.analysis.mr.service.impl.IDimensionConvertImpl;

/**
 * @Author Taten
 * @Description
 **/
public class DimensionTest {
    public static void main(String[] args) {
        PlatformDimension platformDimension = new PlatformDimension("ios");
        IDimensionConvert convert = new IDimensionConvertImpl();
        System.out.println(convert.getDimensionByValue(platformDimension));
    }
}
