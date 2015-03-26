package com.knoldus.meetup

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object WordCount extends App {

  //Turn off spark's default logger
  Logger.getLogger("org").setLevel(Level.OFF)

  val logFile = "src/main/resources/data/CHANGES.txt" // Should be some file on your system
  val conf = new SparkConf().setMaster("local[4]").setAppName("wordCount") // run locally with enough threads
  val sc = new SparkContext(conf)

  val logData = sc.textFile(logFile, 4).cache()
  val words = logData.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_ + _).filter(wc => (wc._2 > 500) && (wc._1 != ""))
  words.foreach(word => println(s"Word: ${word._1}, Count: ${word._2}"))

}
