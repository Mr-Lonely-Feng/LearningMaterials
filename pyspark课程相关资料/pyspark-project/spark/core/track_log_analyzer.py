#!/usr/bin/python
# -*- encoding:utf-8 -*-

"""
@author: xuanyu
@contact: xuanyu@126.com
@file: track_log_analyzer.py
@time: 2017/5/16 21:54
"""

# 导入模块 pyspark
from pyspark import SparkConf, SparkContext
# 导入系统模块
import os, time

if __name__ == '__main__':

    # 设置SPARK_HOME环境变量
    os.environ['SPARK_HOME'] = 'E:/spark-1.6.1-bin-2.5.0-cdh5.3.6'
    os.environ['HADOOP_HOME'] = 'G:/OnlinePySparkCourse/pyspark-project/winuntil'

    # Create SparkConf
    sparkConf = SparkConf()\
        .setAppName('Python Spark WordCount')\
        .setMaster('local[2]')

    # Create SparkContext
    sc = SparkContext(conf=sparkConf)
    # 设置日志级别
    # sc.setLogLevel('WARN')

    """
        Step 1：
            read data : SparkContext从HDFS读取数据
    """
    # file　hdfs directory
    track_log = "/user/hive/warehouse/db_track.db/yhd_log/date=20150828"
    # read data
    track_rdd = sc.textFile(track_log)

    # # test
    # print "Count = " + str(track_rdd.count())
    # print track_rdd.first()

    """
        Step 2: process data
            RDD#Transformation
        需求：
            统计每日PV和UV
                PV：页面的浏览量/访问量
                    pv = COUNT(url)  url不能空, url.length > 0     第2列
                UV：访客数
                    uv = COUNT(DISTINCT guid)                     第6列
                时间：
                    用户访问时间的字段获取
                        tracktime     2015-08-28 18:10:00         第18列
    """
    # 字符串的映射函数
    def split_data_func(line):
        # 字符串分割
        words = line.split("\t")
        # 字符串截取
        date_str = str(words[17])[0:10]
        # return (date, url, guid)
        return date_str, words[1], words[5]

    # 对原始数据进行清洗过滤及转换
    filtered_rdd = track_rdd\
        .filter(lambda line: (len(line.strip()) > 0) and (len(line.split("\t")) > 20))\
        .map(split_data_func)

    # # test
    # print str(filtered_rdd.first())
    # # ('2015-08-28', u'http://www.yhd.com/?union_ref=7&cp=0', u'PR4E9HWE38DMN4Z6HUG667SCJNZXMHSPJRER')

    # 缓存数据
    filtered_rdd.cache()

    """
        统计每日PV
    """
    pv_rdd = filtered_rdd\
        .map(lambda (date, url, guid): (date, url))\
        .filter(lambda (date, url): len(url.strip()) > 0)\
        .map(lambda t: (t[0], 1))\
        .reduceByKey(lambda x, y: x + y)
    # print
    pv_first = pv_rdd.first()
    print "PV: " + str(pv_first[0]) + " = " + str(pv_first[1])
    # PV: 2015-08-28 = 69197
    # FOR 循环打印
    for pv_item in pv_rdd.collect():
        print str(pv_item[0]) + " = " + str(pv_item[1])

    """
        每日统计UV
    """
    uv_rdd = (filtered_rdd
                .map(lambda (date, url, guid): (date, guid))
                .distinct()
                .map(lambda t: (t[0], 1))
                .reduceByKey(lambda x, y: x + y))
    # print
    uv_first = uv_rdd.first()
    print "UV: " + str(uv_first[0]) + " = " + str(uv_first[1])
    # UV: 2015-08-28 = 39007

    # 释放缓存的数据
    filtered_rdd.unpersist()

    """
        RDD#union:
            将两个RDD进行合并,要求两个RDD的类型要一致，如果不一样的话，不行
    """
    union_rdd = pv_rdd.union(uv_rdd)
    print union_rdd.collect()
    # [('2015-08-28', 69197), ('2015-08-28', 39007)]

    # 是不对，运行都没有结果
    # sc.parallelize([1, 2, 3, 4]).union(pv_rdd).collect()

    """
        RDD#join:
            将两个RDD进行连接操作，要求RDD的类型应该是(key, value),依据key相同进行关联
    """
    join_rdd = pv_rdd.join(uv_rdd)
    print join_rdd.collect()
    # [('2015-08-28', (69197, 39007))]

    # WEB UI 4040, 让线程休眠一段时间
    time.sleep(100000)

    # SparkContext Stop
    sc.stop()
