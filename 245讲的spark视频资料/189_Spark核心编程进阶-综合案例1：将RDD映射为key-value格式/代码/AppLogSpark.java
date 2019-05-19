package cn.spark.study.core.upgrade.applog;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

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
		
		// 将RDD映射为key-value格式，为后面的reduceByKey聚合做准备
		JavaPairRDD<String, AccessLogInfo> accessLogPairRDD = 
				mapAccessLogRDD2Pair(accessLogRDD);
		
		// 关闭Spark上下文
		sc.close();
	}
	
	/**
	 * 将日志RDD映射为key-value的格式
	 * @param accessLogRDD 日志RDD
	 * @return key-value格式RDD
	 */
	private static JavaPairRDD<String, AccessLogInfo> mapAccessLogRDD2Pair(
			JavaRDD<String> accessLogRDD) {
		return accessLogRDD.mapToPair(new PairFunction<String, String, AccessLogInfo>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, AccessLogInfo> call(String accessLog)
					throws Exception {
				// 根据\t对日志进行切分
				String[] accessLogSplited = accessLog.split("\t");  
				
				// 获取四个字段
				long timestamp = Long.valueOf(accessLogSplited[0]);
				String deviceID = accessLogSplited[1];
				long upTraffic = Long.valueOf(accessLogSplited[2]);
				long downTraffic = Long.valueOf(accessLogSplited[3]);  
				
				// 将时间戳、上行流量、下行流量，封装为自定义的可序列化对象
				AccessLogInfo accessLogInfo = new AccessLogInfo(timestamp,
						upTraffic, downTraffic);
				
				return new Tuple2<String, AccessLogInfo>(deviceID, accessLogInfo);
			}
			
		});
	}
	
}
