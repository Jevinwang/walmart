#!/bin/bash

source /home/hadoop/.bashrc
path=`pwd`

echo $path

tomorrow="`date -d '1 days' +%Y%m%d`"
dayAfterTom="`date -d '2 days' +%Y%m%d`"
TwoDaysAfterTom="`date -d '3 days' +%Y%m%d`"
generateCmdToFile(){
echo "create 'tb_view_time-$1', {NAME=>'vt',COMPRESSION=>'gz'}, {SPLITS => ['?', '\\\\x80', '\\\\xC0']}" >> ${path}/hbaseShell/hbase-shell-${1}.txt
echo "create 'tb_cart_time-$1', {NAME=>'ct',COMPRESSION=>'gz'}, {SPLITS => ['?', '\\\\x80', '\\\\xC0']}" >> ${path}/hbaseShell/hbase-shell-${1}.txt
#echo "create 'tb_buy_time-$1', {NAME=>'bt',COMPRESSION=>'gz'}, {SPLITS => ['?', '\\\\x80', '\\\\xC0']}" >> ${path}/hbaseShell/hbase-shell-${1}.txt
#echo "create 'AR_order_info-$1', {NAME=>'oi',COMPRESSION=>'gz'}, {SPLITS => ['?', '\\\\x80', '\\\\xC0']}" >> ${path}/hbaseShell/hbase-shell-${1}.txt
echo "create 'log_by_kafka_topic-$1', {NAME=>'log',COMPRESSION=>'gz'}, {SPLITS => ['?', '\\\\x80', '\\\\xC0']}" >> ${path}/hbaseShell/hbase-shell-${1}.txt
#echo "create 'recom_exposure_result-$1', {NAME=>'er',COMPRESSION=>'gz'}, {SPLITS => ['?', '\\\\x80', '\\\\xC0']}" >> ${path}/hbaseShell/hbase-shell-${1}.txt
echo "create 'recom_click_result-$1', {NAME=>'clr',COMPRESSION=>'gz'}, {SPLITS => ['?', '\\\\x80', '\\\\xC0']}" >> ${path}/hbaseShell/hbase-shell-${1}.txt
#echo "create 'recom_cart_result-$1', {NAME=>'car',COMPRESSION=>'gz'}, {SPLITS => ['?', '\\\\x80', '\\\\xC0']}" >> ${path}/hbaseShell/hbase-shell-${1}.txt
echo "exit" >> ${path}/hbaseShell/hbase-shell-${1}.txt
echo "hbase-shell-${1}.txt is created."
}
if [ ! -f "${path}/hbaseShell/hbase-shell-${tomorrow}.txt" ];then
echo "hbase-shell-${tomorrow}.txt does not exist."
generateCmdToFile ${tomorrow}
hbase shell ${path}/hbaseShell/hbase-shell-${tomorrow}.txt
fi
if [ ! -f "${path}/hbaseShell/hbase-shell-${dayAfterTom}.txt" ];then
echo "hbase-shell-${dayAfterTom}.txt does not exist."
generateCmdToFile ${dayAfterTom}
hbase shell ${path}/hbaseShell/hbase-shell-${dayAfterTom}.txt
fi
if [ ! -f "${path}/hbaseShell/hbase-shell-${TwoDaysAfterTom}.txt" ];then
echo "hbase-shell-${TwoDaysAfterTom}.txt does not exist."
generateCmdToFile ${TwoDaysAfterTom}
hbase shell ${path}/hbaseShell/hbase-shell-${TwoDaysAfterTom}.txt
fi
