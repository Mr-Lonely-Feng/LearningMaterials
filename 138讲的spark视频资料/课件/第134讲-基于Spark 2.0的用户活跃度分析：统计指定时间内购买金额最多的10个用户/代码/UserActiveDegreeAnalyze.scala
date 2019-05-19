package cn.ibeifeng.spark

import org.apache.spark.sql.SparkSession

/**
 * 用户活跃度分析
 * 
 * 我们这次项目课程的升级，也跟spark从入门到精通的升级采取同步，采用scala+eclipse的方式来开发
 * 
 * 我个人而言，还是觉得应该用java去开发spark作业，因为hadoop是最重要的大数据引擎，hadoop mapreduce、hbase，全都是java
 * 整个公司的编程语言技术栈越简单越好，降低人员的招聘和培养的成本
 * 
 * 但是由于市面上，现在大部分的公司，做spark都是采取一种，spark用scala开发，所以开发spark作业也用scala
 * 课程为了跟市场保持同步，后面就随便采取scala来开发了
 * 
 */
object UserActiveDegreeAnalyze {
  
  def main(args: Array[String]) {
    // 如果是按照课程之前的模块，或者整套交互式分析系统的架构，应该先从mysql中提取用户指定的参数（java web系统提供界面供用户选择，然后java web系统将参数写入mysql中）
    // 但是这里已经讲了，之前的环境已经没有了，所以本次升级从简
    // 我们就直接定义一个日期范围，来模拟获取了参数
    val startDate = "2016-09-01";
    val endDate = "2016-11-01";
    
    // 开始写代码
    // spark 2.0具体开发的细节和讲解，全部在从入门到精通中，这里不多说了，直接写代码
    // 要不然如果没有看过从入门到精通的话，就自己去上网查spark 2.0的入门资料
    
    val spark = SparkSession
        .builder()
        .appName("UserActiveDegreeAnalyze")
        .master("local") 
        .config("spark.sql.warehouse.dir", "C:\\Users\\Administrator\\Desktop\\spark-warehouse")
        .getOrCreate()
        
    // 导入spark的隐式转换
    import spark.implicits._
    // 导入spark sql的functions
    import org.apache.spark.sql.functions._
    
    // 获取两份数据集
    val userBaseInfo = spark.read.json("C:\\Users\\Administrator\\Desktop\\user_base_info.json")
    val userActionLog = spark.read.json("C:\\Users\\Administrator\\Desktop\\user_action_log.json")
  
    // 第一个功能：统计指定时间范围内的访问次数最多的10个用户
    // 说明：课程，所以数据不会搞的太多，但是一般来说，pm产品经理，都会抽取100个~1000个用户，供他们仔细分析
    
//    userActionLog
//        // 第一步：过滤数据，找到指定时间范围内的数据
//        .filter("actionTime >= '" + startDate + "' and actionTime <= '" + endDate + "' and actionType = 0") 
//        // 第二步：关联对应的用户基本信息数据
//        .join(userBaseInfo, userActionLog("userId") === userBaseInfo("userId")) 
//        // 第三部：进行分组，按照userid和username
//        .groupBy(userBaseInfo("userId"), userBaseInfo("username")) 
//        // 第四步：进行聚合
//        .agg(count(userActionLog("logId")).alias("actionCount")) 
//        // 第五步：进行排序
//        .sort($"actionCount".desc)
//        // 第六步：抽取指定的条数
//        .limit(10) 
//        // 第七步：展示结果，因为监简化了，所以说不会再写入mysql
//        .show()
    
    // 第二个功能：获取指定时间范围内购买金额最多的10个用户
    // 对金额进行处理的函数讲解
    // feature，技术点的讲解：嵌套函数的使用
    userActionLog
        .filter("actionTime >= '" + startDate + "' and actionTime <= '" + endDate + "' and actionType = 1") 
        .join(userBaseInfo, userActionLog("userId") === userBaseInfo("userId")) 
        .groupBy(userBaseInfo("userId"), userBaseInfo("username"))
        .agg(round(sum(userActionLog("purchaseMoney")),2).alias("totalPurchaseMoney")) 
        .sort($"totalPurchaseMoney".desc)
        .limit(10)
        .show() 
  }
  
}