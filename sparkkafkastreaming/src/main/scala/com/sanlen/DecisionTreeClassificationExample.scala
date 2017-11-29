package com.sanlen

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.sql.SQLContext
/**
  * 决策树 回归算法法是机器学习分类和回归问题中非常流行的算法。因其易解释性、可处理类别特征、易扩展到多分
  * 决策树以及其集成算类问题、不需特征缩放等性质被广泛使用。
  * 树集成算法如随机森林以及boosting算法几乎是解决分类和回归问题中表现最优的算法。
  * 决策树是一个贪心算法递归地将特征空间划分为两个部分，在同一个叶子节点的数据最后会拥有同样的标签。
  * 每次划分通过贪心的以获得最大信息增益为目的，
  * 从可选择的分裂方式中选择最佳的分裂节点。节点不纯度有节点所含类别的同质性来衡量。
  * 工具提供为分类提供两种不纯度衡量（基尼不纯度和熵），为回归提供一种不纯度衡量（方差）。
  *spark.ml支持二分类、多分类以及回归的决策树算法，适用于连续特征以及类别特征。另外，对于分类问题，
  * 工具可以返回属于每种类别的概率（类别条件概率），
  * 对于回归问题工具可以返回预测在偏置样本上的方差。
  * author : LuHongGang
  * time   : 2017/11/29
  * version: 1.0
  */
object DecisionTreeClassificationExample {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("TreeClassificationExample").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val spark =new SQLContext(sc)
    //Load the data stored in LIBSVM format as a DataFrame.
    val data = spark.read.format("libsvm").load("D:\\sample_libsvm_data.txt")

    // Automatically identify categorical features, and index them.
    // Here, we treat features with > 4 distinct values as continuous.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(data)

    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    // Train a DecisionTree model.
    val dt = new DecisionTreeRegressor()
      .setLabelCol("label")
      .setFeaturesCol("indexedFeatures")

    // Chain indexer and tree in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(featureIndexer, dt))

    // Train model. This also runs the indexer.
    val model = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
    predictions.select("prediction", "label", "features").show(5)

    // Select (prediction, true label) and compute test error.
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)

    val treeModel = model.stages(1).asInstanceOf[DecisionTreeRegressionModel]
    println("Learned regression tree model:\n" + treeModel.toDebugString)
  }
}
