#!/bin/bash

# proj1测试基本只要10s

rm p1_*.log

fdir='proj1'
rm -rf $fdir
mkdir $fdir


for i in {1..100}
do
fname='p1_'$i'.log'

echo $i >> $fname

start_time=$(date +%s)

make project1 >> $fname

end_time=$(date +%s)
cost_time=$[ $end_time-$start_time ]
echo "共耗时: $(($cost_time/60))min $(($cost_time%60))s"


if cat $fname | grep FAIL  #只要是失败，最后一行一定是FAIL，更宽松点可以说最后两三行中一定有FAIL
then
  echo 'test '$i' failed'
  mv $fname './'$fdir
else
  echo 'test '$i' passed'
  rm $fname
fi
done

# 一定要尝试下写个错误试试！不然可能有问题但是还是认为没问题，毕竟写shell脚本不熟