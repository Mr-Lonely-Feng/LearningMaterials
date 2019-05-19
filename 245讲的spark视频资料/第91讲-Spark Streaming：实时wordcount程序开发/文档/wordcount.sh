/usr/local/spark-1.5.1-bin-hadoop2.4/bin/spark-submit \
--class cn.spark.study.streaming.WordCount \
--num-executors 3 \
--driver-memory 100m \
--executor-memory 100m \
--executor-cores 3 \
/usr/local/spark-study/scala/streaming/spark-study-scala.jar \
