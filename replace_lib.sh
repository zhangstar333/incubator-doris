#!/bin/bash

if [[ $1 = '--be' ]];then
  #cp output/be/lib/palo_be output_copy/be/lib/
  cp output/be/lib/doris_be /home/disk4/zhangstar/doris-vectorized/output_copy/be/lib/
  #cp /home/disk4/zhangstar/incubator-doris/be/output/lib/palo_be /home/disk4/zhangstar/incubator-doris/output/be/lib
  cp output/fe/conf/fe_back.conf output/fe/conf/fe.conf
  cp output/be/conf/be_back.conf output/be/conf/be.conf
  cp output/be/conf/start_be_back.sh output/be/bin/start_be.sh
  #cp output/be/lib/meta_tool output_copy/be/lib/
  echo "--be repalce output_cooy be lib. No meta_tool palo_be.bak"
elif [[ $1 = '--fe' ]];then
  cp output/fe/lib/spark-dpp-0.15-SNAPSHOT.jar output_copy/fe/lib/
  cp output/fe/lib/palo-fe.jar output_copy/fe/lib/
  cp output/fe/lib/fe-common-0.15-SNAPSHOT.jar output_copy/fe/lib/

  echo "--fe repalce output_cooy fe lib. Only 3 files"
elif [[ $1 = '--sh' ]];then
  cp output/fe/conf/fe_back.conf output/fe/conf/fe.conf
  cp output/be/conf/be_back.conf output/be/conf/be.conf
  cp output/be/conf/start_be_back.sh output/be/bin/start_be.sh
  cp output/be/conf/apache_hdfs_broker.conf  output/apache_hdfs_broker/conf/apache_hdfs_broker.conf
  echo "--conf repalce output conf file, And be start.sh"

else
  echo "-- others, Nothing to do"
fi