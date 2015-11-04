package me.play.spark.rdd

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

/**
 * Top 2 actors with maximum movies.
 * @author mangeeteden
 */
object Top2ActorsWithMaxMovies {

  def main(args: Array[String]) {
    val sc = new SparkContext()

    val home = System.getProperty("user.home")
    val movies = sc.textFile(s"$home/videos_by_actor.csv", 3)  // with 3 partitions

    // converting each movie row to pair RDD with date as key and complete row as value
    val moviesByActor = movies.map { movie => (movie.split(",")(0), 1) }
    val moviesCountByEachActor = moviesByActor.reduceByKey((a, b) => a + b)
    val moviesCollectionSortedByCount = moviesCountByEachActor.map(item => item.swap).sortByKey(false)

    println("Top 2 Actors acted in maximum movies: ")
    moviesCollectionSortedByCount.top(2).foreach { case (a, b) => println(b + " - " + a) }
  }
}
