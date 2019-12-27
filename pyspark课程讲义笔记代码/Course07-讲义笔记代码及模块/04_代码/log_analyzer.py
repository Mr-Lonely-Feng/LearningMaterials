#!/usr/bin/python
# -*- encoding:utf-8 -*-

"""
@author: xuanyu
@contact: xuanyu@126.com
"""

# 导入模块 pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, Row
# 导入系统模块
import os
import time

if __name__ == '__main__':

    # 设置SPARK_HOME环境变量
    os.environ['SPARK_HOME'] = 'E:/spark-1.6.1-bin-2.5.0-cdh5.3.6'
    os.environ['HADOOP_HOME'] = 'G:/OnlinePySparkCourse/pyspark-project/winuntil'

    # Create SparkConf
    sparkConf = SparkConf()\
        .setAppName('Python Log Analyzer')\
        .setMaster('local[2]')

    # Create SparkContext
    sc = SparkContext(conf=sparkConf)
    # 设置日志级别
    sc.setLogLevel('WARN')

    # Create SQLContext
    sqlContext = SQLContext(sparkContext=sc)

    # ====================================================================================
    """
        数据准备
            uplherc.upl.com - - [01/Aug/1995:00:00:07 -0400] "GET / HTTP/1.0" 304 0

        字段信息：
            remotehost rfc931 authuser [date] "request" status bytes
    """
    # 日志文件路径
    log_file_path = "/datas/access_log_Jul95"
    # 读取HDFS上文件，此处不适用sc.textFile()此方法，直接使用SparkSQL中方法进行读取数据
    # sqlContext.read.text()方式，读取数据以后仅有一个字段, 名称为：value，类型为String
    base_df = sqlContext.read.text(log_file_path)

    # # 打印展示
    # print "原始数据Count：" + str(base_df.count())
    # # # 原始数据Count：1891715
    # # 样本数据展示
    # base_df.show(n=10, truncate=False)

    """
        数据解析
            将数据进行解析，解析到七个字段中
                - 正则表达式
                - SparkSQL的函数regexp_extract()
                此函数可以使用包含一个或者多个捕获组的正则表打是匹配的内容，然后提取其中一个捕获组来匹配
            解析获取的字段：
                host        timestamp       path        status      content_size
    """
    # 导入相应的函数
    from pyspark.sql.functions import regexp_extract
    # 正则表达式中常用的字符意思
    #   s->任意空白字符  S->任意非空白字符  d->一个数字  D->非数字  w->数字\字母\下划线中任意一个字符
    #   ^->匹配开始位置  $->匹配结束位置
    split_df = base_df.select(
        regexp_extract('value', r'^([^\s]+\s)', 1).alias("host"),
        regexp_extract('value', r'^.*\[(\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]', 1).alias('timestamp'),
        regexp_extract('value', r'^.*"\w+\s+([^\s]+)\s+HTTP.*"', 1).alias('path'),
        regexp_extract('value', r'^.*"\s+([^\s]+)', 1).cast('integer').alias('status'),
        regexp_extract('value', r'^.*\s+(\d+)$', 1).cast('integer').alias('content_size')
    )
    # # 展示数据
    # split_df.show(n=10, truncate=False)
    # print "解析数据Count：" + str(split_df.count())
    # # # 解析数据Count：1891715

    """
        数据清洗过滤
            - 查看一下原始日志数据包含多少空行  -  原始日志
            - 统计有多少行的数据至少有一个字段包含null值  - 解析数据
    """
    # # 查看原始日志有多少空行
    # black_df = base_df.filter(base_df['value'].isNull())
    # print "空行：" + str(black_df.count())
    # # # 空行：0

    # =============================================================
    # # 统计有多少行数据至少包含一个null值
    # bad_rows_df = split_df.filter(
    #     split_df['host'].isNull() |
    #     split_df['timestamp'].isNull() |
    #     split_df['path'].isNull() |
    #     split_df['status'].isNull() |
    #     split_df['content_size'].isNull()
    # )
    # print "行数(至少包含一个null值)：" + str(bad_rows_df.count())
    # # 行数(至少包含一个null值)：19727

    # =============================================================
    # 考虑：到底哪些字段的数据存在null值呢？？？
    # 统计每列有多少个null值
    from pyspark.sql.functions import col, sum

    # 定义函数，统计某个字段为null出现的个数
    def count_null(column_name):
        return sum(col(column_name).isNull().cast('integer')).alias(column_name)
    # 定义列表list
    exprs = []
    for col_name in split_df.columns:
        exprs.append(count_null(col_name))

    # # df.agg(F.min(df.age))
    # split_df.agg(*exprs).show()
    # # +----+---------+----+------+------------+
    # # |host|timestamp|path|status|content_size|
    # # +----+---------+----+------+------------+
    # # |   0|        0|   0|     1|       19727|
    # # +----+---------+----+------+------------+

    """
        针对为null的字段的值进行过滤
            - 对status这列值进行过滤为null的数据
            - 对content_size这列数据为null进行过滤
    """
    cleaned_df_first = split_df.filter(split_df['status'].isNotNull())
    # cleaned_df_first.agg(*exprs).show()
    # # +----+---------+----+------+------------+
    # # |host|timestamp|path|status|content_size|
    # # +----+---------+----+------+------------+
    # # |   0|        0|   0|     0|       19726|
    # # +----+---------+----+------+------------+

    # # 针对最后一列进行过滤，首先统计有多少条数据是不以一个或者多个数字结尾
    # bad_content_size_df = base_df.filter(~ base_df['value'].rlike(r'\d+$'))
    # print "不以一个或者多个数字结尾的行：" + str(bad_content_size_df.count())
    # # 不以一个或者多个数字结尾的行：19727

    # # 看看坏的脏的数据
    # from pyspark.sql.functions import concat, lit
    # bad_content_size_df.select(concat(bad_content_size_df['value']), lit('*')).show(truncate=False)

    # 从上述的显示结果来看，表明数据response客户端的时候，服务器谢了一个dash字符，需要将这些字符替换为0
    # na 返回的是一个DataFrameNaFunctions Object，这个对象中包含了很多可以粗略null列的方法
    cleaned_df = cleaned_df_first.na.fill({'content_size': 0})
    # cleaned_df.agg(*exprs).show()
    # print "清洗数据Count：" + str(cleaned_df.count())
    # # 清洗数据Count：1891714

    """
        数据转换
            现在数据中 timestamp 列并不是实时的时间戳， 而是Apache服务器的时间格式
                例如：01/Aug/1995:00:00:07 -0400
            格式为：
                [dd/mmm/yyyy:hh:mm:ss (+/-)zzzz]
            需求转换为标准的时间戳格式
                [yyyy-MM-dd hh:mm:ss]
    """
    # 定义一个字典
    month_map = {
        'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
        'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
    }

    # 定义函数 01/Aug/1995:00:00:07 -0400
    def parse_clf_time(s):
        return "{0:04d}-{1:02d}-{2:02d} {3:02d}:{4:02d}:{5:02d}".format(
            int(s[7:11]),
            month_map[s[3:6]],
            int(s[0:2]),
            int(s[12:14]),
            int(s[15:17]),
            int(s[18:20]),
        )
    # 使用UDF函数，转换日期数据
    from pyspark.sql.functions import udf
    u_parse_time = udf(parse_clf_time)

    # 进行数据格式转换，采用DataFrame（DSL）
    logs_df = cleaned_df.select(
        '*',
        u_parse_time(cleaned_df['timestamp']).cast('timestamp').alias('time')
    ).drop('timestamp')
    # # 打印
    # logs_df.show(truncate=False)

    """
        数据统计分析：
            -1, 数据总览概要信息
            -2, HTTP响应状态统计
            -3, 客户端访问频率统计
            -4, URIs访问量统计
            -5, 统计HTTP犯规状态不是200的十大URI
            -6, 统计host的数量
            -7, 统计每天host访问量
            -8, 日均host访问量
            -9, 404状态数据分析
    """

    # WEB UI 4040, 让线程休眠一段时间
    time.sleep(100000)

    # SparkContext Stop
    sc.stop()
