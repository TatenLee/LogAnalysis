1. 创建维度类，并创建对应的udf方法
create function payment_convert as 'com.bigdata.analysis.hive.PaymentTypeDimensionUdf' using jar "hdfs://hadoop01:9000//logs/udf/jars/LogAnalysis-1.0-SNAPSHOT.jar";
create function currency_convert as 'com.bigdata.analysis.hive.CurrencyDimensionUdf' using jar "hdfs://hadoop01:9000//logs/udf/jars/LogAnalysis-1.0-SNAPSHOT.jar";

2. 创建表
create table if not exists stats_order_tmp(
    `date_dimension_id` int,
    `platform_dimension_id` int,
    `currency_type_dimension_id` int,
    `payment_type_dimension_id` int,
    `ct` int,
    `created` string
);

3. 写指标

总的订单数量：

from(
select
from_unixtime(cast(l.s_time/1000 as bigint), "yyyy-MM-dd") as dt,
l.pl as pl,
l.cut as cut,
l.pt as pt,
count(distinct l.o_id) as ct
from logs l
where l.month = 05
and l.day=28
and l.o_id is not null
and l.o_id <> 'null'
group by from_unixtime(cast(1.s_time/1000 as bigint), "yyyy-MM-dd"), l.pl, l.cut, l.pt, ct
)
insert overwrite table stats_order_tmp
select convert_date(dt), convert_platorm(pl), convert_pay(pt), convert_currency(cut), sum(ct), dt
group by dt, pl, cut, pt