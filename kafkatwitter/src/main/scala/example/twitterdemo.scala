// twitterdemo.scala
// https://medium.com/analytics-vidhya/finding-the-urgent-blood-plasma-requirements-on-twitter-using-spark-streaming-959ebb6f5590

// scp -P 2222 jars\*.jar maria_dev@sandbox-hdp.hortonworks.com:/home/maria_dev
// spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0,net.liftweb:lift-json_2.11:3.5.0,org.twitter4j:twitter4j-core:3.0.6,org.twitter4j:twitter4j-stream:3.0.6,org.apache.spark:spark-streaming-twitter_2.11:1.6.3 --jars ./spark-core_2.11-1.5.2.logging.jar kafkatwitter_2.11-0.1.0-SNAPSHOT.jar --class example.twitterdemo
package example

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.rdd.RDD
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth.OAuthAuthorization

//import Utilities._

/** Listens to a stream of Tweets and filters the ones related to urgent blood requirement
 *  hashtags over a 5 minute window.
 */
object twitterdemo {
  def main(args: Array[String]) {
    /* Set up a Spark streaming context named "DonateBlood" that runs locally using
    all CPU cores and one-second batches of data. */
    val ssc = new StreamingContext("local[*]", "DonateBlood", Seconds(1))
    ssc.sparkContext.setLogLevel("ERROR")

    // Configure Twitter credentials.
    val auth = setupTwitter()

    // Create a list of filter keywords to pass as arguments while creating twitter stream.
    val filters = List(
      "blood",
      "plasma",
      "#blood",
      "#bloodrequired",
      "#BloodMatters",
      "#BloodDrive",
      "#DonateBlood",
      "#Blood",
      "#NeedBlood"
    )
    
    // Create a DStream from Twitter using the Spark streaming context and the filters
    val tweets = TwitterUtils.createStream(ssc, Some(auth), filters)

    // Now extract the id, date, user,text,location, retweet, hashtags of each status into DStreams using map().
    val statuses = tweets.map { status =>
      val id = status.getUser.getId
      val user = status.getUser.getName
      val date = status.getCreatedAt.toString
      val text = status.getText
      val location = status.getUser.getLocation
      val retweet = status.isRetweet()

      //For extracting hashtags, create a list of words by splitting the text of the status using ' '(space) and then filter
      // only the words that start with #
      val hashtags =
        status.getText.split(" ").toList.filter(word => word.startsWith("#"))
      
      (id, date, user, text, location, retweet, hashtags)
    }

    // Path for storing the solution file.
    val path = "twitterdemo/plasma.parquet"

    // foreachRDD applies the 'function' inside it to each RDD generated from the stream. 
    // This function should push the data in each RDD to an external system (in this case, a parquet file and a temp view).
    statuses.foreachRDD((rdd, time) => {
      val spark = SparkSession.builder().appName("AjaySingalaTwitterDemo").getOrCreate()

      import spark.implicits._
      
      // Filter out empty batches.
      if (rdd.count() > 0) {
        // Convert rdd to Dataframe.
        val requestsDataFrame = rdd.toDF(
          "id",
          "date",
          "user",
          "text",
          "location",
          "retweet",
          "hashtags"
        )
        val bloodDonateTweets = requestsDataFrame
          .filter(
            col("text").contains("urgent") || col("text")
              .contains("need") || col("text").contains("emergency") || col(
              "text"
            ).contains("required")
          )
          .dropDuplicates("text")
        
        // Filter out empty batches.
        if (bloodDonateTweets.count() > 0) {
          bloodDonateTweets.createOrReplaceTempView("BloodDonationTable")

          // Create a SQL table from this DataFrame.
          val solution = spark.sqlContext.sql(
            "select * from BloodDonationTable order by date"
          )
          solution.show(false)

          // Write the data into a parquet file.
          solution.coalesce(1).write.mode(SaveMode.Append).parquet(path)
        }
      }
    })

    // Set a checkpoint directory.
    ssc.checkpoint("/tmp/twitter_checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }

  def setupTwitter() = {
    // import scala.io.Source
    // for (line <- Source.fromFile("/home/maria_dev/twitter.txt").getLines) {
    //   val fields = line.split(" ")
    //   fields.foreach(println)
    //   if (fields.length == 2) {
    //     System.setProperty("twitter4j.oauth." + fields(0), fields(1))
    //   }
    // }
    val _consumerKey = ""
    val _consumerSecret = ""
    val _accessToken = ""
    val _accessTokenSecret = ""
    
    var args: Array[String] = new Array[String](6)
    args(0)= _consumerKey
    args(1) = _consumerSecret
    args(2) = _accessToken
    args(3) = _accessTokenSecret

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)

    val cb = new ConfigurationBuilder
    
    println("Configuring Twitter Auth...")
    cb.setDebugEnabled(true).setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)

    println("OAuth init...")
    val auth = new OAuthAuthorization(cb.build)

    auth
  }
}
