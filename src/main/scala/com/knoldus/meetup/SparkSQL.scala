package com.knoldus.meetup

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

// Schema for people RDD
case class Person(name: String, age: Int)

object SparkSQL extends App {

  // Turn off spark's default logger
  Logger.getLogger("org").setLevel(Level.OFF)

  val file = "src/main/resources/data/people.txt" // Should be some file on your system
  val conf = new SparkConf().setMaster("local[4]").setAppName("sparkSQL") // run locally with enough threads
  val sc = new SparkContext(conf)
  val data = sc.textFile(file, 4)

  // Create context for Spark SQL
  val sqlContext = new SQLContext(sc)

  // Importing the SQL context gives access to all the SQL functions and implicit conversions.
  import sqlContext._

  // Convert records of the RDD (people) to Rows.
  val people = data.map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))

  // Register the SchemaRDD as a table.
  people.registerTempTable("people")

  // SQL statements can be run by using the sql methods provided by sqlContext.
  val results = sqlContext.sql("SELECT name FROM people")
  results.map(t => "Name: " + t(0)).collect().foreach(println)

}
