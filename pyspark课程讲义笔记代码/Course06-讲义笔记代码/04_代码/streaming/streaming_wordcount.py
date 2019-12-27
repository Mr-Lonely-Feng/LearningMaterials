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
from pyspark.streaming import StreamingContext
# 导入系统模块
import os
import time

if __name__ == '__main__':

    # 设置SPARK_HOME环境变量
    os.environ['SPARK_HOME'] = 'E:/spark-1.6.1-bin-2.5.0-cdh5.3.6'
    os.environ['HADOOP_HOME'] = 'G:/OnlinePySparkCourse/pyspark-project/winuntil'

    # Create SparkConf
    sparkConf = SparkConf()\
        .setAppName('Python Spark WordCount')\
        .setMaster('local[3]')

    # Create SparkContext
    sc = SparkContext(conf=sparkConf)
    # 设置日志级别
    sc.setLogLevel('WARN')

    # Create a local StreamingContext with two working thread and batch interval of 5 second
    ssc = StreamingContext(sc, batchDuration=5)

    # 从Sockect 接收数据，进行处理
    lines_dstream = ssc.socketTextStream('bigdata-training01.hpsk.com', 9999)

    # 每批次的词频统计
    words_dstream = lines_dstream.flatMap(lambda line: line.split(' '))
    # 统计
    pairs_dstream = words_dstream.map(lambda word: (word, 1))
    word_count_dstream = pairs_dstream.reduceByKey(lambda x, y: x + y)

    # print
    word_count_dstream.pprint()

    """
        需求：
            自定义输出，将数据写入到Redis Cluster（基于内存NoSQL分布式数据库，以Key/value存储数据）
            - 题外话：
                如何获取SparkStreaming处理数据的每批次时间，
                        Key： batchTime
                        Value：output-RDD
    """

    # 自定义函数，用于处理结果
    def print_func(batch_time, rdd):
        taken = rdd.collect()
        print("-------------------------------------------")
        print("Time: %s" % batch_time)
        print("-------------------------------------------")
        for record in taken:
            print record

    # 自定义结果的处理
    word_count_dstream.foreachRDD(print_func)

    # 启动Streaming应用
    ssc.start()
    ssc.awaitTermination()

    # WEB UI 4040, 让线程休眠一段时间
    time.sleep(100000)

    # SparkContext Stop
    ssc.stop()
