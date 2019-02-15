#! /bin/bash

kinit -kt vn0uvnl.keytab vn0uvnl@CNHADOOP.WAL-MART.COM

exec spark-shell --queue online --name spark-createTable <<!EOF
import org.apache.spark.sql.SaveMode
sql("use cn_omni_uid")
sql("create table dwd_test(id STRING,uid_start STRING,uid STRING,wid STRING,source int) partitioned by (inser_day STRING) stored as ORC")
!EOF
