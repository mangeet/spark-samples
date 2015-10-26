package me.play.spark

import org.apache.spark.SparkContext
import java.io.File

/**
 * Count the number of movies per each added date.
 * @author mangeeteden
 */
object Top10DatesOnWhichMostMoviesAdded {

  def main(args: Array[String]) {
    val sc = new SparkContext()

    val home = System.getProperty("user.home")
    val movies = sc.textFile(s"$home/videos_by_actor.csv")

    // converting each movie row to pair RDD with date as key and complete row as value
    val moviesByDate = movies.map { movie => (movie.split(",")(1), 1) }
    val moviesCountByEachDate = moviesByDate.reduceByKey((a, b) => a + b)
    val moviesCollectionSortedByCount = moviesCountByEachDate.map(item => item.swap).sortByKey(false)

    println("Top 10 dates with maximum movied added: ")
    moviesCollectionSortedByCount.top(10).foreach { case (a, b) => println(b + " - " + a) }
  }
}
