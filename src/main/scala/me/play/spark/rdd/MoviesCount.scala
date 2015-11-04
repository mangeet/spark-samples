package me.play.spark.rdd

import org.apache.spark.SparkContext

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
