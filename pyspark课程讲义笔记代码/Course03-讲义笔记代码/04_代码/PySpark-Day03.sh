
# spark application running spark standalone
bin/spark-submit \
--master spark://bigdata-training01.hpsk.com:7077 \
--deploy-mode client \
--driver-memory 512M \
--executor-memory 1G \
--executor-cores 1 \
--total-executor-cores 2 \
--conf “spark.ui.port=5050” \
/opt/cdh-5.3.6/spark-1.6.1-bin-2.5.0-cdh5.3.6/spark_word_count.py




