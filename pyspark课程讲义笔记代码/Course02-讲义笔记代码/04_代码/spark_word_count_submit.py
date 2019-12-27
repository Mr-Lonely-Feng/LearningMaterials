#!/usr/bin/python
# -*- encoding:utf-8 -*-

"""
@author: xuanyu
@contact: xuanyu@126.com
@file:.py
@time:2017/5/16 21:54
"""

# 导入模块 pyspark
from pyspark import SparkConf, SparkContext
# 导入系统模块
import os
import time

if __name__ == '__main__':

    # 设置SPARK_HOME环境变量, 非常重要, 如果没有设置的话,SparkApplication运行不了
    os.environ['SPARK_HOME'] = '/opt/cdh-5.3.6/spark-1.6.1-bin-2.5.0-cdh5.3.6'

    # Create SparkConf
    sparkConf = SparkConf()\
        .setAppName('Python Spark WordCount')

    # Create SparkContext
    sc = SparkContext(conf=sparkConf)

    # 第二种方式：从HDFS分布式文件系统读取数据，创建RDD
    rdd = sc.textFile("/datas/wc.input")

    # ===============================================================
    # 词频统计，针对SCALA编程 flatMap\map\reduceByKey
    word_count_rdd = rdd\
        .flatMap(lambda line: line.split(" "))\
        .map(lambda word: (word, 1))\
        .reduceByKey(lambda a, b: a + b)
    # output
    for x in word_count_rdd.collect():
        print x[0] + ", " + str(x[1])

    # =============================================================
    # 使用RDD#top进行获取topKey
    top_word_count = word_count_rdd\
        .top(3, key=lambda (word, count): count)
    # output
    for (top_word, top_count) in top_word_count:
        print ("%s: %i" % (top_word, top_count))

    # WEB UI 4040, 让线程休眠一段时间
    time.sleep(100000)

    # SparkContext Stop
    sc.stop()
