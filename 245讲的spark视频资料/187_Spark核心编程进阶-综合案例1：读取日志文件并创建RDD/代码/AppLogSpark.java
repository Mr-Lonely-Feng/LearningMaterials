package cn.spark.study.core.upgrade.applog;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 移动端app访问流量日志分析案例
 * @author Administrator
 *
 */
public class AppLogSpark {

	public static void main(String[] args) throws Exception {
		// 创建Spark配置和上下文对象
		SparkConf conf = new SparkConf()
				.setAppName("AppLogSpark")  
				.setMaster("local"); 
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// 读取日志文件，并创建一个RDD
		// 使用SparkContext的textFile()方法，即可读取本地磁盘文件，或者是HDFS上的文件
		// 创建出来一个初始的RDD，其中包含了日志文件中的所有数据
		JavaRDD<String> accessLogRDD = sc.textFile(
				"C://Users//Administrator//Desktop//access.log");   
		
		// 关闭Spark上下文
		sc.close();
	}
	
}
