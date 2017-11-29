package com.sanlen

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.ml.feature.PCA
import org.apache.spark.mllib.linalg.Vectors
/**
  *  分词器  博客链接[http://blog.csdn.net/liulingyuan6/article/details/53397780]
  *  算法介绍：
    *Tokenization将文本划分为独立个体（通常为单词）。下面的例子展示了如何把句子划分为单词。
    *RegexTokenizer基于正则表达式提供更多的划分选项。默认情况下，参数“pattern”为划分文本的分隔符。
    *或者，用户可以指定参数“gaps”来指明正则“patten”表示“tokens”而不是分隔符，这样来为分词结果找到所有可能匹配的情况。
  * author : LuHongGang
  * time   : 2017/11/29
  * version: 1.0
  */
object TokenizerArithmetic {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("TokenizerArithmetic").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val sentenceDataFrame = sqlContext.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (1, "I wish Java could use case classes"),
      (2, "Logistic,regression,models,are,neat")
    )).toDF("label", "sentence")
    println("------------->>>打印数据开始<<<--------------")
    sentenceDataFrame.show()

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("sentence")
      .setOutputCol("words")
      .setPattern("\\W") // alternatively .setPattern("\\w+").setGaps(false)

    val tokenized = tokenizer.transform(sentenceDataFrame)
    tokenized.select("words", "label").take(3).foreach(println)
    val regexTokenized = regexTokenizer.transform(sentenceDataFrame)
    println("------------->>>打印数据结果<<<--------------")
    regexTokenized.show()

    /**
      * import org.apache.spark.ml.linalg.Vectors
      * PCA
        算法介绍：
        主成分分析是一种统计学方法，它使用正交转换从一系列可能相关的变量中提取线性无关变量集，
        提取出的变量集中的元素称为主成分。使用PCA方法可以对变量集合进行降维。
       下面的示例将会展示如何将5维特征向量转换为3维主成分向量。
      */
    val data = Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    )
    val df = sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(3)
      .fit(df)
    val pcaDF = pca.transform(df)
    val result = pcaDF.select("pcaFeatures")
    println("------------->>>打印PCA算法结果<<<--------------")
    result.show()
  }

}
