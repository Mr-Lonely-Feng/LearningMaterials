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
    sc.setLogLevel('WARN')

    # WEB UI 4040, 让线程休眠一段时间
    time.sleep(100000)

    # SparkContext Stop
    sc.stop()
