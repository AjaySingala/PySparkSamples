// twitterstreaming.scala
// https://dzone.com/articles/twitter-live-streaming-with-spark-streaming-using
// https://towardsdatascience.com/how-to-capture-and-store-tweets-in-real-time-with-apache-spark-and-apache-kafka-e5ccd17afb32
// http://flummox-engineering.blogspot.com/2014/06/sbt-use-jar-file-for-librarydependencies.html

// scp -P 2222 jars\*.jar maria_dev@sandbox-hdp.hortonworks.com:/home/maria_dev
// spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0,net.liftweb:lift-json_2.11:3.5.0,org.twitter4j:twitter4j-core:3.0.6,org.twitter4j:twitter4j-stream:3.0.6,org.apache.spark:spark-streaming-twitter_2.11:1.6.3 --jars ./spark-core_2.11-1.5.2.logging.jar kafkatwitter_2.11-0.1.0-SNAPSHOT.jar --class example.twitterstreaming
package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization
import twitter4j.Status
import org.apache.spark.streaming.twitter.TwitterUtils

object twitterstreaming {
  def main_ts(args: Array[String]) {
    // if (args.length < 4) {
    //   System.err.println("Usage: TwitterData <ConsumerKey><ConsumerSecret><accessToken><accessTokenSecret>" +
    //     "[<filters>]")

    //   System.exit(1)
    // }

    val _consumerKey = ""
    val _consumerSecret = ""
    val _accessToken = "179804560-"
    val _accessTokenSecret = ""
    
    var args: Array[String] = new Array[String](6)
    args(0)= _consumerKey
    args(1) = _consumerSecret
    args(2) = _accessToken
    args(3) = _accessTokenSecret
    args(4) = "Bitcoin"
    args(5) = "Doge"

    val appName = "TwitterData"
    val conf = new SparkConf()
    conf.setAppName(appName).setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(20))
    ssc.sparkContext.setLogLevel("ERROR")

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)

    val filters = args.takeRight(args.length - 4)
    val cb = new ConfigurationBuilder
    
    println("Configuring Twitter Auth...")
    cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)

    println("OAuth init...")
    val auth = new OAuthAuthorization(cb.build)

    println(s"Fetching tweets for...")
    filters.foreach(println)
    val tweets = TwitterUtils.createStream(ssc, Some(auth), filters)
    
    val outputPath = "twitterOutput/tweets"
    println(s"Saving tweets to $outputPath")
    tweets .saveAsTextFiles(outputPath, "json")
    ssc.start()
    ssc.awaitTermination()
  }
}