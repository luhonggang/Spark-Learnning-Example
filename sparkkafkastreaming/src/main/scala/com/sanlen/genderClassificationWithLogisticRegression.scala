package com.sanlen
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization._
import org.apache.spark.mllib.regression.LabeledPoint
/**
  * author : LuHongGang
  * time   : 2017/11/29
  * version: 1.0
  */
object genderClassificationWithLogisticRegression {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("genderClassification").setMaster("local[2]")
    val sc = new SparkContext(conf)
    // 1.读取数据
    val trainData = sc.textFile("file:\\E:\\user_test.txt")

    // 2.解析数据，构建数据集
    val parsedTrainData = trainData.map { line =>
      val parts= line.split("\\|")
      val label = toInt(parts(1)) //第二列是标签
    val features = Vectors.dense(parts.slice(6,parts.length-1).map(_.toDouble)) //第7到最后一列是属性，需要转换为Doube类型
      LabeledPoint(label, features) //构建LabelPoint格式,第一列是标签列，后面是属性向量
    }.cache()

    // 3.将数据集随机分为两份，一份是训练集，一份是测试集
    val splits = parsedTrainData.randomSplit(Array(0.7, 0.3), seed = 11L)
    val training = splits(0)
    val testing = splits(1)
    println("第一份数据是 : "+training +" 第二份数据是 : "+testing)

    // 4.新建逻辑回归模型，并设置训练参数
    //    val model = new LogisticRegressionWithLBFGS().setNumClasses(2)
    //    model.optimizer.setNumIterations(500).setUpdater(new SimpleUpdater())

    //可以选择LogisticRegressionWithLBFGS，也可以选择LogisticRegressionWithSGD，LogisticRegressionWithLBFGS是优化方法
    val model = new LogisticRegressionWithSGD()  //建立模型
    model.optimizer.setNumIterations(500).setUpdater(new SimpleUpdater()).setStepSize(0.001).setMiniBatchFraction(0.02) //模型参数

    val trained = model.run(training)  //使用训练集训练模型

    // 5.测试样本进行预测
    val prediction = trained.predict(testing.map(_.features)) //使用测试数据属性进行预测

    val predictionAndLabels = prediction.zip(testing.map(_.label)) //获取预测标签

    // 6.测量预测效果
    val metrics = new BinaryClassificationMetrics(predictionAndLabels)

    // 7.看看AUROC结果
    val auROC = metrics.areaUnderROC
    println("Area under ROC = " + auROC)
  }

  // 将标签转换为0和1
  def toInt(s: String): Int = {
    if (s == "m") 1 else  0
  }
}
