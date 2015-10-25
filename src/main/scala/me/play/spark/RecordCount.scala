package me.play.spark

import org.apache.spark.SparkContext
import java.io.File

/**
 * @author mangeeteden
 */
object RecordCount {
  
  def main(args : Array[String]) {
    val sc = new SparkContext()
    
    val home = System.getProperty("user.home")
    val lines = sc.textFile(s"$home/videos_by_actor.csv")
    
    val lineCount = lines.count()
    println("Number of lines: " + lineCount)
  }
}
