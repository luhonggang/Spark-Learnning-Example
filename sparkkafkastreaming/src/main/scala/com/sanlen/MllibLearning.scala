package com.sanlen
import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
// $example off$

/**
  * author : LuHongGang
  * time   : 2017/11/28
  * version: 1.0
  */
object MllibLearning {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("MllibLearning").setMaster("local")
    val sc = new SparkContext(sparkConf)
   // val linesRDD =sc.textFile("C:\\Users\\Administrator\\Desktop\\human.txt").map(_.split(":"))
   // $example on$
   val input = sc.textFile("D:\\BaiduNetdiskDownload\\spark源码\\spark-master\\data\\mllib\\sample_lda_data.txt").map(line => line.split(" ").toSeq)

    val word2vec = new Word2Vec()

    val model = word2vec.fit(input)

    val synonyms = model.findSynonyms("1", 5)

    for((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
    }

    // Save and load model
    model.save(sc, "myModelPath")
    val sameModel = Word2VecModel.load(sc, "myModelPath")
    // $example off$

    sc.stop()
    /**
      *
      */
//    // Create a dense vector (1.0, 0.0, 3.0).
//    val dv: Vector = Vectors.dense(1.0, 0.0, 3.0)
//    // Create a sparse vector (1.0, 0.0, 3.0) by specifying its indices and values corresponding to nonzero entries.
//    val sv1: Vector = Vectors.sparse(3, Array(0, 2), Array(1.0, 3.0))
//    // Create a sparse vector (1.0, 0.0, 3.0) by specifying its nonzero entries.
//    val sv2: Vector = Vectors.sparse(3, Seq((0, 1.0), (2, 3.0)))
//
//    println(sv1 +" "+sv2)

  }

}
