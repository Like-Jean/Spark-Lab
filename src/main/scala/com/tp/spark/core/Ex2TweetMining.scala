
package com.tp.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import com.tp.spark.utils._
import com.tp.spark.utils.TweetUtils.Tweet

import Ex0Wordcount.sc
/**
 *
 *  We still use the dataset with the 8198 reduced tweets. Here an example of a tweet:
 *
 *  {"id":"572692378957430785",
 *    "user":"Srkian_nishu :)",
 *    "text":"@always_nidhi @YouTube no i dnt understand bt i loved of this mve is rocking",
 *    "place":"Orissa",
 *    "country":"India"}
 *
 *  We want to make some computations on the tweets:
 *  - Find all the persons mentioned on tweets
 *  - Count how many times each person is mentioned
 *  - Find the 10 most mentioned persons by descending order
 *
 */
object Ex2TweetMining {

  val pathToFile = "data/reduced-tweets.json"

  /**
   *  Load the data from the json file and return an RDD of Tweet
   */
  
  def loadData(): RDD[Tweet] = {

    // Load the data and parse it into a Tweet.
    // Look at the Tweet Object in the TweetUtils class.
    sc.textFile(pathToFile)
        .mapPartitions(TweetUtils.parseFromJson(_))

  }

  /**
   *  Find all the persons mentioned on tweets (case sensitive)
   */
  def mentionOnTweet(): RDD[String] = {
    val data = loadData
    
    data.flatMap { x => x.text.split(' ')}
      .filter { x => x.startsWith("@") & x.length() > 1}
  }

  /**
   *  Count how many times each person is mentioned
   */
  def countMentions(): RDD[(String, Int)] = {
    val data = loadData
    data.flatMap { x => x.text.split(' ')}
      .filter { x => x.startsWith("@") & x.length() > 1}
      .map { tweet => (tweet,1) }
      .reduceByKey(_+_)
    
  }

  /**
   *  Find the 10 most mentioned persons by descending order
   */
  def top10mentions(): Array[(String, Int)] = {
    val data = loadData
    data.flatMap { x => x.text.split(' ')}
      .filter { x => x.startsWith("@") & x.length() > 1}
      .map { tweet => (tweet,1) }
      .reduceByKey(_+_)
      .sortBy(f => -f._2)
      .collect().slice(0, 10)
  }

}
