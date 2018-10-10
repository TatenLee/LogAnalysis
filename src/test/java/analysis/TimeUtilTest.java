package analysis;

import com.bigdata.common.DateEnum;
import com.bigdata.util.TimeUtil;

public class TimeUtilTest {
    public static void main(String[] args) {
//        System.out.println(TimeUtil.isValidateDate("2018-08-16"));
//        System.out.println(TimeUtil.getYesterday());
//        System.out.println(TimeUtil.parseString2Long("2018-08-17"));
//        System.out.println(TimeUtil.parseLong2String(1534435200000L));
        System.out.println(TimeUtil.getDateInfo(1534435200000L, DateEnum.DAY));
        System.out.println(TimeUtil.getDateInfo(1534435200000L, DateEnum.WEEK));
        System.out.println(TimeUtil.getDateInfo(1534435200000L, DateEnum.MONTH));
        System.out.println(TimeUtil.getFirstDayOfWeek(1534435200000L));
    }
}
