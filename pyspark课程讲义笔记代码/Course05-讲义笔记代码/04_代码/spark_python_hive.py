#!/usr/bin/python
# -*- encoding:utf-8 -*-

"""
@author: xuanyu
@contact: xuanyu@126.com
"""

# 导入模块 pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
# 导入系统模块
import os
import time

if __name__ == '__main__':

    # 设置SPARK_HOME环境变量
    os.environ['SPARK_HOME'] = '/opt/cdh-5.3.6/spark-1.6.1-bin-2.5.0-cdh5.3.6'
    os.environ['HADOOP_HOME'] = '/opt/tools/pyspark-project/winuntil'

    # Create SparkConf
    sparkConf = SparkConf()\
        .setAppName('Python SparkSQL Hive')\
        .setMaster('local[2]')

    # Create SparkContext
    sc = SparkContext(conf=sparkConf)
    # 设置日志级别
    sc.setLogLevel('WARN')

    # Create HiveContext
    sqlContext = HiveContext(sparkContext=sc)

    """
        Read Data From Hive Table
    """
    # emp table
    emp_df = sqlContext.read.table("db_hive.emp")

    """
        Read Data From MySQL Table
    """
    # dept table
    dept_df = sqlContext\
        .read\
        .jdbc('jdbc:mysql://bigdata-training01.hpsk.com:3306/test', 'tb_dept', properties={'user': 'root', 'password': '123456'})

    # # print
    # emp_df.printSchema()
    # dept_df.show(truncate=False)

    # emp join dept
    join_result = emp_df\
        .join(dept_df, 'deptno')\
        .select('empno', 'sal', 'dname')\
        .take(14)
    for result in join_result:
        print str(result['empno']) + "\t" + str(result['sal']) + '\t' + str(result['dname'])

    # WEB UI 4040, 让线程休眠一段时间
    time.sleep(100000)

    # SparkContext Stop
    sc.stop()
