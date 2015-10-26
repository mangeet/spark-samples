package me.play.spark

import org.apache.spark.SparkContext
import java.io.File

/**
 * Counts the number of movies available in source file.
 * @author mangeeteden
 */
object MoviesCount {

  def main(args: Array[String]) {
    val sc = new SparkContext()

    val home = System.getProperty("user.home")
    val movies = sc.textFile(s"$home/videos_by_actor.csv")

    val moviesCount = movies.count()
    println("Number of movies: " + moviesCount)
  }
}
