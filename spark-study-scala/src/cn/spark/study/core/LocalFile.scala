package cn.spark.study.core

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author Administrator
 */
object LocalFile {
  
  def main(args: Array[String]) {
    val conf = new SparkConf()
        .setAppName("LocalFile") 
        .setMaster("local");  
    val sc = new SparkContext(conf)
    
    val lines = sc.textFile("C://Users//Administrator//Desktop//spark.txt", 1);
    val count = lines.map { line => line.length() }.reduce(_ + _)  
    
    println("file's count is " + count)  
  }
  
}