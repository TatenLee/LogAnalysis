#!/bin/bash

#./en.sh -n -m -d 2018-07-27
run_date=
until [ $# -eq 0 ]
do
if [ $1'x' = '-dx' ]
then
shift
run_date=$1
fi
shift
done


if [ ${#run_date} != 10 ]
then
run_date=`date -d "1 days ago" "+%Y-%m-%d"`
else
echo "$run_date"
fi

month=`date -d "$run_date" "+%m"`
day=`date -d "$run_date" "+%d"`
echo "final running date is:${run_date},${month},${day}"

###############################################
########run hql statment
########
##############################################


hive --database lda -e "
from ods_logs
insert overwrite table dw_dp partition(month=${month},day=${day})
select s_time,pl,p_url,u_ud,u_sid
where month = ${month}
and day = ${day}
;
"

hive --database lda -e "
from(
select
from_unixtime(cast(dd.s_time/1000 as bigint),'yyyy-MM-dd') dt,
dd.pl pl,
(case
when count(dd.p_url) = 1 then 'pv1'
when count(dd.p_url) = 2 then 'pv2'
when count(dd.p_url) = 3 then 'pv3'
when count(dd.p_url) = 4 then 'pv4'
when count(dd.p_url) <= 10  then 'pv5_10'
when count(dd.p_url) <= 30 then 'pv10_30'
when count(dd.p_url) <= 60 then 'pv30_60'
else 'pv60pluss'
end) as pv,
dd.u_ud u_ud
from dw_dp dd
where dd.pl is not null
and dd.p_url is not null
and dd.month = ${month}
and dd.day = ${day}
group by pl,from_unixtime(cast(dd.s_time/1000 as bigint),'yyyy-MM-dd'),u_ud
) as tmp
insert overwrite table dwa_dp
select pl,dt,pv,count(u_ud)
group by pl,dt,pv
;
"

hive --database lda -e "
with tmp as(
select pl as pl,dt,ct as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col='pv1' union all
select pl as pl,dt,0 as pv1,ct as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col='pv2' union all
select pl as pl,dt,0 as pv1,0 as pv2,ct as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col='pv3' union all
select pl as pl,dt,0 as pv1,0 as pv2,0 as pv3,ct as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col='pv4' union all
select pl as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,ct as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col='pv5_10' union all
select pl as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,ct as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col='pv10_30' union all
select pl as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,ct as pv30_60,0 as pv60pluss from dwa_dp where col='pv30_60' union all
select pl as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,ct as pv60pluss from dwa_dp where col='pv60pluss' union all
select 'all' as pl,dt,ct as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col='pv1' union all
select 'all' as pl,dt,0 as pv1,ct as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col='pv2' union all
select 'all' as pl,dt,0 as pv1,0 as pv2,ct as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col='pv3' union all
select 'all' as pl,dt,0 as pv1,0 as pv2,0 as pv3,ct as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col='pv4' union all
select 'all' as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,ct as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col='pv5_10' union all
select 'all' as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,ct as pv10_30,0 as pv30_60,0 as pv60pluss from dwa_dp where col='pv10_30' union all
select 'all' as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,ct as pv30_60,0 as pv60pluss from dwa_dp where col='pv30_60' union all
select 'all' as pl,dt,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,ct as pv60pluss from dwa_dp where col='pv60pluss'
)
from tmp
insert overwrite table stats_view_depth
select date_convert(dt),platform_convert(pl),2,sum(pv1),sum(pv2),sum(pv3),sum(pv4),sum(pv5_10),sum(pv10_30),sum(pv30_60),sum(pv60pluss),dt
group by pl,dt
;
"

echo "run sqoop statment..."
sqoop export --connect jdbc:mysql://hadoop01:3306/result \
--username root --password root --table stats_view_depth \
--export-dir hdfs://hadoop01:9000/hive/lda.db/stats_view_depth/* \
--input-fields-terminated-by "\\001" --update-mode allowinsert \
--update-key platform_dimension_id,data_dimension_id,kpi_dimension_id \
;

echo "view depth job is finished."