package com.knoldus.meetup

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.log4j.Logger
import org.apache.log4j.Level

object SparkMLLib extends App {

  //Turn off spark's default logger
  Logger.getLogger("org").setLevel(Level.OFF)

  val conf = new SparkConf().setMaster("local[4]").setAppName("mllib") // run locally with enough threads
  val sc = new SparkContext(conf)

  val data = sc.textFile("src/main/resources/data/sample_naive_bayes_data.txt", 4)
  val parsedData = data.map { line =>
    val parts = line.split(',')
    LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
  }
  // Split data into training (60%) and test (40%).
  val splits = parsedData.randomSplit(Array(0.6, 0.4))
  val training = splits(0)
  val test = splits(1)

  val model = NaiveBayes.train(training, lambda = 1.0)
  val prediction = model.predict(test.map(_.features))

  val predictionAndLabel = prediction.zip(test.map(_.label))
  val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
  println("Accuracy = " + accuracy * 100 + "%")

}
