package cn.spark.study.streaming.upgrade.news;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

/**
 * 新闻网站关键指标实时统计Spark应用程序
 * @author Administrator
 *
 */
public class NewsRealtimeStatSpark {

	public static void main(String[] args) throws Exception {
		// 创建Spark上下文
		SparkConf conf = new SparkConf()
				.setMaster("local[2]")
				.setAppName("NewsRealtimeStatSpark");  
		JavaStreamingContext jssc = new JavaStreamingContext(
				conf, Durations.seconds(5));  
		
		// 创建输入DStream
		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", 
				"192.168.0.103:9092,192.168.0.104:9092");
		
		Set<String> topics = new HashSet<String>();
		topics.add("news-access");  
		
		JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(
				jssc, 
				String.class, 
				String.class, 
				StringDecoder.class, 
				StringDecoder.class, 
				kafkaParams, 
				topics);
		
		// 过滤出访问日志
		JavaPairDStream<String, String> accessDStream = lines.filter(
				
				new Function<Tuple2<String,String>, Boolean>() {
			
					private static final long serialVersionUID = 1L;
		
					@Override
					public Boolean call(Tuple2<String, String> tuple) throws Exception {
						String log = tuple._2;
						String[] logSplited = log.split(" ");  
						
						String action = logSplited[5];
						if("view".equals(action)) {
							return true;
						} else {
							return false;
						}
					}
					
				});
		
		// 统计第一个指标：每10秒内的各个页面的pv
		calculatePagePv(accessDStream);  
		
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}
	
	/**
	 * 计算页面pv
	 * @param accessDStream
	 */
	private static void calculatePagePv(JavaPairDStream<String, String> accessDStream) {
		JavaPairDStream<Long, Long> pageidDStream = accessDStream.mapToPair(
				
				new PairFunction<Tuple2<String,String>, Long, Long>() {

					private static final long serialVersionUID = 1L;
		
					@Override
					public Tuple2<Long, Long> call(Tuple2<String, String> tuple)
							throws Exception {
						String log = tuple._2;
						String[] logSplited = log.split(" "); 
						
						Long pageid = Long.valueOf(logSplited[3]);  
						
						return new Tuple2<Long, Long>(pageid, 1L);  
					}
					
				});
		
		JavaPairDStream<Long, Long> pagePvDStream = pageidDStream.reduceByKey(
				
				new Function2<Long, Long, Long>() {
			
					private static final long serialVersionUID = 1L;
		
					@Override
					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}
					
				});
		
		pagePvDStream.print();  
		
		// 在计算出每10秒钟的页面pv之后，其实在真实项目中，应该持久化
		// 到mysql，或redis中，对每个页面的pv进行累加
		// javaee系统，就可以从mysql或redis中，读取page pv实时变化的数据，以及曲线图
	}
	
}
