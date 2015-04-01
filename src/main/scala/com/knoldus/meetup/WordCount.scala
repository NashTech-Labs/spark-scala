package com.knoldus.meetup

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level

/**
 * Calculates word-count of a text file.
 */
object WordCount extends App {

  //Turn off spark's default logger
  Logger.getLogger("org").setLevel(Level.OFF)

  val file = "src/main/resources/data/CHANGES.txt" // Should be some file on your system
  val conf = new SparkConf().setMaster("local[4]").setAppName("wordCount") // run locally with enough threads
  val sc = new SparkContext(conf)

  // Calculating the word-count
  val data = sc.textFile(file, 4)
  val words = data
    .flatMap(_.split(" ")).map(word => (word, 1))
    .reduceByKey(_ + _).filter(wc => (wc._2 > 500) && (wc._1 != "")) // Filtering out final word-counts

  //Printing out word-count
  words.foreach(word => println(s"Word: ${word._1}, Count: ${word._2}"))

}
