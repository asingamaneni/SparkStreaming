package com.asingamaneni.sparkstreaming

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import Utilities._

/** This program listens to the stream of tweets and saves them to the disk */
object SaveTweets {
  
  /** Main function where the actual action happens */
  def main(args: Array[String]): Unit = {
    
    // Configure Twitter credentials using twitter.txt
    //Call the method from Utilities.scala 
    setupTwitter()
    
    // Configure Spark Streaming Context names "SaveTweets" that runs locally using 
    // all the CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "SaveTweets", Seconds(1))
    
    // Get rid of log spam (should be called after the context is set up)
    // Call the method from Utilities.scala
    setupLogging()
    
    // Create Dstream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)
    
    // Now extract the text of each status update into RDD's using map()
    val statuses = tweets.map(status => status.getText())
    
    // Here's the simple way to to dump every partition of every stream to individual files:
    // statuses.saveAsTextFiles("Tweets", "txt")
    
    
    // keep count of how many Tweet's we have received so we can stop automatically,
    // so as to not fill up the disk space
    var totalTweets : Long = 0
    
    statuses.foreachRDD((rdd, time) => {
     // Do not consider the empty batches 
      if (rdd.count() > 0) {
        // Combine each partition's results into a single RDD:
        val repartitionedRDD = rdd.repartition(1).cache()
        // and print out a directory with the results/
        repartitionedRDD.saveAsTextFile("Tweets_"+ time.milliseconds.toString())
        // stop once we've collected 1000 tweets.
        totalTweets += repartitionedRDD.count()
        println("Tweet Count: " + totalTweets)
        if (totalTweets > 1000) {
          System.exit(0)
          //ssc.stop()
        }
      }
      
    })
      
    
    ssc.checkpoint("C:/checkpoint")
    ssc.start()
    ssc.awaitTermination()
    
  }
}