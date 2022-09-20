import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.SaveMode
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.TimestampType

object TwitterStreamingAnalysis {


  def main(args: Array[String]) {

    if (args.length < 4) {
      System.err.println("Usage: TwitterStreamingApp <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val path = "s3:a//twitterData//tweets.json"
    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)


    val filters = List("IKEA", "ikea", "Ikea")
    val sparkConf = new SparkConf().setAppName("TwitterAnalysis")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None, filters)

    val statuses = stream.map{status =>
      val id = status.getUser.getId
      val created_date = status.getCreatedAt.toString
      val text = status.getText
      val retweet = status.isRetweet()
      val hashtags = status.getText.split(" ").toList.filter(word =>  word.startsWith("#"))
      (id, created_date,text, retweet, hashtags)
    }

    statuses.foreachRDD((rdd, time) =>
    {

      import spark.implicits._
      if (rdd.count() > 0){
        val requestsDataFrame =  rdd.toDF("id","created_date","text","retweet","hashtags").select(col("id"), col("created_date").cast(TimestampType),col("text"),col("retweet"),col("hashtags"))
        requestsDataFrame.createOrReplaceTempView("TweetsTable")
        val solution = spark.sqlContext.sql("select * from TweetsTable order by created_date")
        solution.coalesce(1).write.mode(SaveMode.Append).json(path)
        }
      }
    )

    ssc.checkpoint("//checkpoint//")
    ssc.start()
    ssc.awaitTermination()
  }




}
