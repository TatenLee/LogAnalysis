用户角度下的浏览深度:

先统计pv的值:分组  日期, pl, 用户

将统计的pv的值和对应的pv1, pv2等存储到临时表

dt string,
pl string,
col string,
value ''

数据抽取(抽取该模块中需要的字段即可):
load data inpath '/dos/month=08/day=18' into table logs partition(month=08, day=18);

考虑是否需要udf函数:

创建表:
CREATE TABLE IF NOT EXISTS stats_view_depth(
platform_dimension_id int,
data_dimension_id int,
kpi_dimension_id int,
pv1 int,
pv2 int,
pv3 int,
pv4 int,
pv5_10 int,
pv10_30 int,
pv30_60 int,
pv60plus int,
created string
);

创建临时表:
CREATE TABLE IF NOT EXISTS stats_view_depth_tmp(
dt string,
pl string,
col string,
ct int
);

sql语句:
from(
select
from_unixtime(cast(l.s_time/1000 as bigint),"yyyy-MM-dd") as dt,
l.pl as pl,
l.u_ud as uid,
(case
when count(l.p_url) = 1 then "pv1"
when count(l.p_url) = 2 then "pv2"
when count(l.p_url) = 3 then "pv3"
when count(l.p_url) = 4 then "pv4"
when count(l.p_url) < 10 then "pv5_10"
when count(l.p_url) < 30 then "pv10_30"
when count(l.p_url) < 60 then "pv30_60"
else "pv60pluss"
end) as pv
from logs l
where month = 08
and day = 18
and l.p_url <> 'null'
and l.pl is not null
group by from_unixtime(cast(l.s_time/1000 as bigint),"yyyy-MM-dd"),pl,u_ud
) as tmp
insert overwrite table stats_view_depth_tmp
select dt,pl,pv,count(distinct uid) as ct
where uid is not null
group by dt,pl,pv
;

with tmp as(
select dt,pl as pl,ct as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv1' union all
select dt,pl as pl,0 as pv1,ct as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv2' union all
select dt,pl as pl,0 as pv1,0 as pv2,ct as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv3' union all
select dt,pl as pl,0 as pv1,0 as pv2,0 as pv3,ct as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv4' union all
select dt,pl as pl,0 as pv1,0 as pv2,0 as pv3,0 as pv4,ct as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv5_10' union all
select dt,pl as pl,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,ct as pv10_30,0 as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv10_30' union all
select dt,pl as pl,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,ct as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv30_60' union all
select dt,pl as pl,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,ct as pv60pluss from stats_view_depth_tmp where col = 'pv60pluss' union all
select dt,'all' as pl,ct as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv1' union all
select dt,'all' as pl,0 as pv1,ct as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv2' union all
select dt,'all' as pl,0 as pv1,0 as pv2,ct as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv3' union all
select dt,'all' as pl,0 as pv1,0 as pv2,0 as pv3,ct as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv4' union all
select dt,'all' as pl,0 as pv1,0 as pv2,0 as pv3,0 as pv4,ct as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv5_10' union all
select dt,'all' as pl,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,ct as pv10_30,0 as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv10_30' union all
select dt,'all' as pl,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,ct as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv30_60' union all
select dt,'all' as pl,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,ct as pv60pluss from stats_view_depth_tmp where col = 'pv60pluss'
)
from tmp
insert overwrite table stats_view_depth
select convert_date(dt),convert_platform(pl),3,sum(pv1),sum(pv2),sum(pv3),sum(pv4),sum(pv5_10),sum(pv10_30),sum(pv30_60),sum(pv60pluss),dt
group by dt,pl
;



from(
select
from_unixtime(cast(l.s_time/1000 as bigint),"yyyy-MM-dd") as dt,
l.pl as pl,
l.u_ud as uid,
(case
when count(l.p_url) = 1 then "pv1"
when count(l.p_url) = 2 then "pv2"
when count(l.p_url) = 3 then "pv3"
when count(l.p_url) = 4 then "pv4"
when count(l.p_url) < 10 then "pv5_10"
when count(l.p_url) < 30 then "pv10_30"
when count(l.p_url) < 60 then "pv30_60"
else "pv60pluss"
end) as pv
from logs l
where month = 08
and day = 18
and l.p_url <> 'null'
and l.pl is not null
group by from_unixtime(cast(l.s_time/1000 as bigint),"yyyy-MM-dd"),pl,u_ud
) as tmp
insert overwrite table stats_view_depth_tmp
select dt,pl,pv,count(distinct uid) as ct
where uid is not null
group by dt,pl,pv
;

with tmp as(
select dt,pl as pl,ct as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv1' union all
select dt,pl as pl,0 as pv1,ct as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv2' union all
select dt,pl as pl,0 as pv1,0 as pv2,ct as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv3' union all
select dt,pl as pl,0 as pv1,0 as pv2,0 as pv3,ct as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv4' union all
select dt,pl as pl,0 as pv1,0 as pv2,0 as pv3,0 as pv4,ct as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv5_10' union all
select dt,pl as pl,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,ct as pv10_30,0 as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv10_30' union all
select dt,pl as pl,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,ct as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv30_60' union all
select dt,pl as pl,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,ct as pv60pluss from stats_view_depth_tmp where col = 'pv60pluss' union all
select dt,'all' as pl,ct as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv1' union all
select dt,'all' as pl,0 as pv1,ct as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv2' union all
select dt,'all' as pl,0 as pv1,0 as pv2,ct as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv3' union all
select dt,'all' as pl,0 as pv1,0 as pv2,0 as pv3,ct as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv4' union all
select dt,'all' as pl,0 as pv1,0 as pv2,0 as pv3,0 as pv4,ct as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv5_10' union all
select dt,'all' as pl,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,ct as pv10_30,0 as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv10_30' union all
select dt,'all' as pl,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,ct as pv30_60,0 as pv60pluss from stats_view_depth_tmp where col = 'pv30_60' union all
select dt,'all' as pl,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,ct as pv60pluss from stats_view_depth_tmp where col = 'pv60pluss'
)
from (
     from logs l
     where month = 08
     and day = 18
     and l.p_url <> 'null'
     and l.pl is not null
     group by from_unixtime(cast(l.s_time/1000 as bigint),"yyyy-MM-dd"),pl,u_ud
     )
insert overwrite table (select
                       from_unixtime(cast(l.s_time/1000 as bigint),"yyyy-MM-dd") as dt,
                       l.pl as pl,
                       l.u_ud as uid,
                       (case
                       when count(l.p_url) = 1 then "pv1"
                       when count(l.p_url) = 2 then "pv2"
                       when count(l.p_url) = 3 then "pv3"
                       when count(l.p_url) = 4 then "pv4"
                       when count(l.p_url) < 10 then "pv5_10"
                       when count(l.p_url) < 30 then "pv10_30"
                       when count(l.p_url) < 60 then "pv30_60"
                       else "pv60pluss"
                       end) as pv
                       from logs l
                       where month = 08
                       and day = 18
                       and l.p_url <> 'null'
                       and l.pl is not null
                       )
                       group by from_unixtime(cast(l.s_time/1000 as bigint),"yyyy-MM-dd"),pl,u_ud
select convert_date(dt),convert_platform(pl),3,sum(pv1),sum(pv2),sum(pv3),sum(pv4),sum(pv5_10),sum(pv10_30),sum(pv30_60),sum(pv60pluss),dt
group by dt,pl
;