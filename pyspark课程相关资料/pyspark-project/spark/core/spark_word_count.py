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
    os.environ['SPARK_HOME'] = 'E:/spark-1.6.1-bin-2.5.0-cdh5.3.6'

    # HADOOP在Windows的兼容性问题
    os.environ['HADOOP_HOME'] = 'G:/OnlinePySparkCourse/pyspark-project/winuntil'

    # Create SparkConf
    sparkConf = SparkConf()\
        .setAppName('Python Spark WordCount')\
        .setMaster('local[2]')

    # Create SparkContext
    """
        在使用pyspark编程运行程序时,依赖于py4j模块，为什么？？？？
        由于在运行的时候，SparkContext Python API创建的对象 通过 py4j(python for java)转换
    为JavaSparkContext，然后调度Spark Application.
    """
    sc = SparkContext(conf=sparkConf)
    # 设置日志级别
    # sc.setLogLevel('WARN')

    """
        创建RDD
            方式一：从本地集合进行并行化创建
            方式二：从外部文件系统读取数据（HDFS，Local）
    """
    # # 第一种方式：从集合并行化创建RDD
    # # 列表list
    # datas = ['hadoop spark', 'spark hive spark sql', 'spark hadoop sql sql spark']
    # # Create RDD
    # rdd = sc.parallelize(datas)

    # 第二种方式：从HDFS分布式文件系统读取数据，创建RDD
    """
        需要将HDFS Client 配置文件当到 ${SPARK_HOME}/conf 下面，以便程序运行读取信息
    """
    rdd = sc.textFile("/datas/wc.input")

    # 测试, 获取总数count及第一条数据
    print rdd.count()
    print rdd.first()

    # ===============================================================
    # 词频统计，针对SCALA编程 flatMap\map\reduceByKey
    word_count_rdd = rdd\
        .flatMap(lambda line: line.split(" "))\
        .map(lambda word: (word, 1))\
        .reduceByKey(lambda a, b: a + b)
    # output
    for x in word_count_rdd.collect():
        print x[0] + ", " + str(x[1])

    # Save Result To HDFS
    # word_count_rdd.saveAsTextFile("/datas/pyspark-wc-output")

    # =============================================================
    # 获取词频出现次数最多的单词，前三个单词
    sort_rdd = word_count_rdd\
        .map(lambda (word, count): (count, word))\
        .sortByKey(ascending=False)
    # output
    for x in sort_rdd.take(3):
        print x[1] + ", " + str(x[0])

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
