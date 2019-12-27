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
        字段信息：
            |host|timestamp|path|status|content_size|
    """
    # ####################### -1, 数据总览概要信息 ################################
    # # 看一下content_size这一列数据统计指标值
    # content_size_summary_df = logs_df.describe(['content_size'])
    # content_size_summary_df.show(truncate=False)
    # # +-------+------------------+
    # # |summary|content_size      |
    # # +-------+------------------+
    # # |count  |1891714           |
    # # |mean   |20455.509390425825|
    # # |stddev |76957.41545010889 |
    # # |min    |0                 |
    # # |max    |6823936           |
    # # +-------+------------------+
    #
    # # 可以使用自带的函数进行统计
    # from pyspark.sql import functions as sql_function
    # # 使用函数进行统计分析
    # content_size_state = logs_df.agg(
    #     sql_function.avg(logs_df['content_size']),
    #     sql_function.max(logs_df['content_size']),
    #     sql_function.min(logs_df['content_size'])
    # ).first()
    # print 'Using SQL functions:'
    # print 'Count Size Avg:{0:,.2f}; Min: {2:,.0f}; Max: {1:,.0f}'.format(*content_size_state)

    # ####################### -2, HTTP响应状态统计 ################################
    """
        |host|timestamp|path|status|content_size|
        分析：
            status 分组， 统计， 排序
    """
    # # 分组， 统计，排序
    # status_to_count_df = logs_df.groupBy('status').count().sort('status')
    # # 获取有多少个HTTP RESPONSE CODE
    # status_to_count_length = status_to_count_df.count()
    # # 打印信息
    # print 'Fount %d response code' % status_to_count_length
    # # 展示结果
    # status_to_count_df.show()
    #
    # # 可视化展示结果，使用到 柱状图

    # ####################### -3, 客户端访问频率统计 ################################
    """
        统计一下访问服务器次数超过10次host
    """
    # # -1, 分组\统计
    # host_sum_df = logs_df.groupBy('host').count().sort('count', ascending=False)
    # # -2, 过滤\筛选字段
    # host_more_than_10_df = host_sum_df\
    #     .filter(host_sum_df['count'] > 10)\
    #     .select(host_sum_df['host'])
    # # -3, 打印
    # print 'Top 20 hosts that have accessed more than 10 times:\n'
    # host_more_than_10_df.show(truncate=False)

    # ####################### -4, URIs访问量统计 ################################
    """
        统计服务区资源的访问量，首先按照path进行分组，然后计数
    """
    # # -1, 分组、统计、排序
    # paths_df = (logs_df
    #             .groupBy('path')
    #             .count()
    #             .sort('count', ascending=False)
    #             )
    # # -2, 结果转换
    # paths_counts = (paths_df
    #                 .select('path', 'count')
    #                 .map(lambda r: (r[0], r[1]))
    #                 .collect()
    #                 )
    # # -3, 显示前20 URI
    # print paths_df.count()
    # paths_df.show(truncate=False)
    # for uri_count in paths_counts:
    #     print uri_count

    # ####################### -5, 统计HTTP返回状态不是200的十大URI ################################
    """
        首先查询所有的 status <> 200 的记录， 然后按照path进行分组统计排序，显示结果
    """
    # # -1, 过滤数据
    # not_200_df = logs_df.filter('status<>200')
    # # -2, 分组统计排序
    # logs_sum_df = (not_200_df
    #                .groupBy('path')
    #                .count()
    #                .sort('count', ascending=False)
    #                )
    # # -3, 显示前10条数据
    # print 'Top ten Failed URIs：'
    # logs_sum_df.show(10, False)

    # ####################### -6, 统计host的数量 ################################
    """
        潜在条件：对host进行去重
    """
    # # 方式一：使用distinct进行去重
    # print logs_df.select('host').distinct().count()
    #
    # # 方式二：使用dropDuplicates进行去重
    # unique_host_count = logs_df.dropDuplicates(['host']).count()
    # print 'Unique hosts: {0}'.format(unique_host_count)

    # ####################### -7, 统计每天host访问量 ################################
    """
        |host|timestamp|path|status|content_size|
        统计每日的访客数（根据host去重）
            -1, 选择 timestamp 和 host 两列值
            -2, 将同一天的host相同进行去重
            -3, 最后按照day进行分组统计
    """
    # 导入模块函数
    from pyspark.sql.functions import dayofmonth
    # timestamp 提取day
    day_to_host_pair_df = logs_df.select(logs_df.host, dayofmonth(logs_df.time).alias('day'))
    # 去重
    day_group_host_df = day_to_host_pair_df.distinct()
    # 分组统计
    daily_host_df = day_group_host_df.groupBy('day').count().cache()
    # #
    # print 'Unique hosts per day: '
    # daily_host_df.show(30, False)

    # ####################### -8, 日均host访问量 ################################
    """
        统计每天 各个host平均访问量
            -1, 统计出每天总访问量
                将结果存储在total_req_pre_day_df中
            -2, 统计每天的host访问数
                第七个需求已经完成
            -3, 每天的总访问量 / 每天的总host = 日均的host访问量

    """
    # -1, 统计出每天的总访问量, host day count
    total_req_pre_day_df=logs_df\
        .select(logs_df.host, dayofmonth(logs_df.time).alias('day'))\
        .groupBy('day')\
        .count()
    # -2, 注册每日总访问量为 临时表
    total_req_pre_day_df.registerTempTable('total_req_pre_day_df')
    # -3, 注册 每日host统计 为临时表
    daily_host_df.registerTempTable('daily_host_df')
    # -4, 编写SQL进行关联分析
    sql = 'SELECT ' \
          't1.day AS day, t1.count / t2.count AS avg_host_pv ' \
          'FROM '\
          'total_req_pre_day_df t1, daily_host_df t2 '\
          'WHERE t1.day = t2.day'
    # -5, SQL分析
    avg_daily_req_host_df = sqlContext.sql(sql)
    # -6, 打印结果
    print 'Avg number of daily requests per Host is: \n'
    avg_daily_req_host_df.show()

    # #######################  -9, 404状态数据分析 ################################
    # -1, 先来看看 日志中有多少HTTP的响应是404， 使用filter函数进行过滤即可
    not_found_df = logs_df.filter('status=404')
    not_found_df.cache()
    print 'Found {0} 404 URLs: '.format(not_found_df.count())
    # Found 10845 404 URLs:

    # -2, 看看有哪些 URIs, 返回的HTTP 404, 此处需要考虑去重
    not_found_paths_df = not_found_df.select('path')
    # 去重
    unique_not_found_paths_df = not_found_paths_df.distinct()
    # 打印信息
    print '404 URIs: \n'
    unique_not_found_paths_df.show(n=40, truncate=False)

    # -3, 统计返回HTTP 404状态最多的20个URIs，按照path进行分组，进行count统计，降序排序
    # 分组统计排序
    top_20_not_found_df = (not_found_paths_df
                           .groupBy('path')
                           .count()
                           .sort('count', ascending=False)
                           )
    # 打印
    print 'Top Twenty 404 URIs:\n'
    top_20_not_found_df.show(n=20, truncate=False)

    # -4, 统计收到HTTP 404状态的最多的25个hosts
    # 分组\统计\排序
    hosts_404_count_df = (not_found_df
                          .select('host')
                          .groupBy('host')
                          .count()
                          .sort('count', ascending=False)
                          )
    # 显示
    print 'Top 25 hosts that generated errors: \n'
    hosts_404_count_df.show(n=25, truncate=False)

    # -5, 统计每天出现HTTP 404的次数
    from pyspark.sql.functions import dayofmonth
    errors_by_date_df = (not_found_df
                         .select(dayofmonth('time').alias('day'))
                         .groupBy('day')
                         .count()
                         )
    #
    print '404 Error by day:\n'
    errors_by_date_df.show(n=30, truncate=False)

    # -6, 统计哪5天出现HTTP 404次数最多
    top_error_date_df = errors_by_date_df.sort('count', ascending=False)
    print 'Top Five Dates for 404 Requests:\n'
    top_error_date_df.show(n=5, truncate=False)

    # -7, 既然能够按照每天HTTP 404出现次数计息给哪个统计，也可以按照小时进行统计
    from pyspark.sql.functions import hour
    hour_records_sorted_df = (not_found_df
                              .select(hour('time').alias('hour'))
                              .groupBy('hour')
                              .count()
                              .sort('count', ascending=False)
                              )
    print 'Top hours for 404 Requests:\n'
    hour_records_sorted_df.show(n=24, truncate=False)

    # WEB UI 4040, 让线程休眠一段时间
    time.sleep(100000)

    # SparkContext Stop
    sc.stop()
