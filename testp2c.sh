#!/bin/bash

rm p2c_*.log

fdir='proj2c'
rm -rf $fdir
mkdir $fdir


for i in {1..100}
do
fname='p2c_'$i'.log'

echo $i >> $fname

start_time=$(date +%s)

make project2c >> $fname

end_time=$(date +%s)
cost_time=$[ $end_time-$start_time ]
echo "共耗时: $(($cost_time/60))min $(($cost_time%60))s"


if cat $fname | grep FAIL  # 如果中途有一个case出错，好像不一定会在最后显示错误，所以还是要在全文中搜索是否有FAIL
then
  echo 'test '$i' failed'
  mv $fname './'$fdir
else
  echo 'test '$i' passed'
  rm $fname
fi
done

# 一定要尝试下写个错误试试！不然可能有问题但是还是认为没问题，毕竟写shell脚本不熟