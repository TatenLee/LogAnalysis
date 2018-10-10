#!/bin/bash

# ./en.sh -d 2017-05-28

dt=''
# 循环运行时所带的参数
until [ $# -eq 0 ]
do
if [ $1'x' = '-dx' ]
then
shift
dt=$1
fi
shift
done

month=
day=
# 判断日期是否合法和正常
if [ ${#dt} = 10 ]
then
echo "dt:$dt"
else
dt='date -d "1 days age" "+%Y-%m-%d"'
fi

# 计算month和day
month='date -d "$dt" "+$m"'
day='date -d "$dt" "+$d"'
echo "running date is: $dt, month is: $month, day is: $day"
echo "running hive SQL statement..."

# run hive hql
hive --datebase default -e

# run sqoop statement

echo "running event job finished."
