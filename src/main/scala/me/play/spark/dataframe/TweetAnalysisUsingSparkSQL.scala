package me.play.spark.dataframe

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import java.nio.ByteBuffer
import org.apache.spark.sql.hive.HiveContext

/**
 * Uses Spark SQL to analyze tweets like user of max retweet count etc
 * @author mangeeteden
 */
object TweetAnalysisUsingSparkSQL {

  def main(args: Array[String]): Unit = {

    // Spark Configurations to configure Cassandra Host(user name and password if required)
    val conf = new SparkConf(true).setAppName("TweetAnalyticsUsingSparkSQL").set("spark.cassandra.connection.host", "127.0.0.1");
    val sc = new SparkContext(conf)

    // Connect with cassandra keyspace and table to get Cassandra Rows
    val cassandraRows = sc.cassandraTable("play", "tweets")
    val tweetsJsonRDD = cassandraRows.map { row => new String(row.get[ByteBuffer]("tweet").array()) }

    // building HiveContext and converting RDD -> DataFrame
    val hc = new HiveContext(sc)
    val tweetJsonDataFrame = hc.jsonRDD(tweetsJsonRDD)

    // Registering DataFrame as Temp Table, so that we can execute Queries on that
    tweetJsonDataFrame.registerTempTable("tweets")
    tweetJsonDataFrame.cache()

    // selecting user info
    val tweetUserInfoDataFrame = hc.sql("select text, user.id, user.name, user.description from tweets")
    println("Here is the first row: " + tweetUserInfoDataFrame.first())

    // max retweet count
    val maxRetweetCountDataFrame = hc.sql("select id, user.id, user.name, max(retweet_count) as max_retween_count from tweets group by id, user.id, user.name order by max_retween_count desc")
    println("Here is the max count: " + maxRetweetCountDataFrame.first())
  }
}