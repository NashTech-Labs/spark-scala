package com.knoldus.meetup

import org.apache.spark.streaming.{ Seconds, StreamingContext }
import StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import twitter4j.TwitterFactory
import twitter4j.auth.AccessToken
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.apache.log4j.Level

object TwitterPopularTags extends App {

  //Turn off spark's default logger
  Logger.getLogger("org").setLevel(Level.OFF)

  val configuration = ConfigFactory.load

  // Twitter Authentication credentials
  val consumerKey = configuration.getString("consumer_key")
  val consumerSecret = configuration.getString("consumer_secret")
  val accessToken = configuration.getString("access_token")
  val accessTokenSecret = configuration.getString("access_token_secret")

  // Authorising with your Twitter Application credentials
  val twitter = new TwitterFactory().getInstance()
  twitter.setOAuthConsumer(consumerKey, consumerSecret)
  twitter.setOAuthAccessToken(new AccessToken(accessToken, accessTokenSecret))

  val conf = new SparkConf().setMaster("local[4]").setAppName("firstSparkApp") // run locally with enough threads

  val ssc = new StreamingContext(conf, Seconds(10))
  val filters = Seq("#CWC15")
  val stream = TwitterUtils.createStream(ssc, Option(twitter.getAuthorization()), filters)

  val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

  val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
    .map { case (topic, count) => (count, topic) }
    .transform(_.sortByKey(false))

  topCounts10.foreachRDD(rdd => {
    val topList = rdd.take(5)
    println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
    topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
  })

  ssc.start()
  ssc.awaitTermination()

}
