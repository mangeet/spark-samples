package me.play.spark

import org.apache.spark.SparkContext
import java.io.File

/**
 * Count the number of movies per each added date.
 * @author mangeeteden
 */
object MoviesCountByActor {

  def main(args: Array[String]) {
    val sc = new SparkContext()

    val home = System.getProperty("user.home")
    val movies = sc.textFile(s"$home/videos_by_actor.csv")

    // converting each movie row to pair RDD with date as key and complete row as value
    val moviesByActor = movies.map { movie => (movie.split(",")(0), 1) }
    val moviesCountByEachActor = moviesByActor.reduceByKey((a, b) => a+b)
    val moviesCountBySortedActor = moviesCountByEachActor.sortByKey()
    moviesCountBySortedActor.saveAsTextFile(s"$home/MoviesCountByActor")
    println("Collected count of all movies by date in file." + s"$home/MoviesCountByActor")
  }
}
