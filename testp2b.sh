#!/bin/bash

rm p2b_*.log

fdir='proj2b'
rm -rf $fdir
mkdir $fdir


for i in {1..100}
do
fname='p2b_'$i'.log'

echo $i >> $fname

start_time=$(date +%s)

make project2b >> $fname

end_time=$(date +%s)
cost_time=$[ $end_time-$start_time ]
echo "共耗时: $(($cost_time/60))min $(($cost_time%60))s"


if cat $fname | grep FAIL  
then
  echo 'test '$i' failed'
  mv $fname './'$fdir
else
  echo 'test '$i' passed'
  rm $fname
fi
done
