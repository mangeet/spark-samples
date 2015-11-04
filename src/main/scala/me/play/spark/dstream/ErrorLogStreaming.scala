package me.play.spark.dstream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

/**
 * @author mangeeteden
 */
object ErrorLogStreaming {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf(true).setAppName("ErrorLogStreaming")
    val ssc = new StreamingContext(conf, Seconds(1))
    val lines = ssc.socketTextStream("localhost", 8080)
    val errorLines = lines.filter { x => x.contains("error") }
    errorLines.print

    ssc.start()

    ssc.awaitTermination()
  }
}