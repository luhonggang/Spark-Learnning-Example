package cn.spark.study.core;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

/**
 * 并行化集合创建RDD
 * 案例：累加1到10
 * @author Administrator
 *
 */
public class ParallelizeCollection {
	
	public static void main(String[] args) {
		// 创建SparkConf
		SparkConf conf = new SparkConf()
				.setAppName("ParallelizeCollection")
				.setMaster("local");  
		
		// 创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// 要通过并行化集合的方式创建RDD，那么就调用SparkContext以及其子类，的parallelize()方法
		List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
		
		// 执行reduce算子操作
		// 相当于，先进行1 + 2 = 3；然后再用3 + 3 = 6；然后再用6 + 4 = 10。。。以此类推
		int sum = numberRDD.reduce(new Function2<Integer, Integer, Integer>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer num1, Integer num2) throws Exception {
				return num1 + num2;
			}
			
		});
		
		// 输出累加的和
		System.out.println("1到10的累加和：" + sum);  
		
		// 关闭JavaSparkContext
		sc.close();
	}
	
}
