spark-submit \
--master yarn \
--queue online \
--deploy-mode cluster \
--driver-memory 4g \
--num-executors 8 \
--executor-cores 8 \
--executor-memory 64g \
--class com.walmart.uid.Write_Hive \
--conf "spark.ui.port=4041" \
--conf "spark.hadoop.validateOutputSpecs=false" \
--conf "spark.shuffle.consolidateFiles=true" \
/home/vn0uvnl/uid.jar \
/user/vn0uvnl/day20181225/inputpair/test1pair/ \
