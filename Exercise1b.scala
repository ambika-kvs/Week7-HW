package Week7

import org.apache.spark._
import org.apache.log4j.{Level, Logger}

import java.nio.charset.CodingErrorAction
import scala.io.{Codec, Source}
/** Find the movies with the most ratings. */
object Exercise1b {
  /** Our main function where the action happens */
  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new  SparkConf().setMaster("local[*]").setAppName("PopularMovies").set("spark.driver.host", "localhost");
    // Create a SparkContext using every core of the local machine, named PopularMovies
    //alternative: val sc = new SparkContext("local[*]", "PopularMovies")
    val sc = new SparkContext(conf)
    // Read in each rating line
    val lines = sc.textFile("ml-100k/u.data")
    // Map to (movieID, 1) tuples
    val movies = lines.map(x => (x.split("\t")(1).toInt, 1))
    // Count up all the 1's for each movie
    val movieCounts = movies.reduceByKey( (x, y) => x + y )
    //    val movieCounts = movies.reduceByKey(_ + _)

    // Flip (movieID, count) to (count, movieID)
    val flipped = movieCounts.map( x => (x._2, x._1) )
    // Sort
    val sortedMovies = flipped.sortByKey()
    // Collect and print results
    val results = sortedMovies.collect()
    results.foreach(println)

    // Create a broadcast variable of our ID -> movie name map
    val nameDict = sc.broadcast(loadMovieNames())

    val sortedMoviesWithNames = sortedMovies.map(x => (nameDict.value(x._2), x._1))
    sortedMoviesWithNames.foreach(println(_))
  }

  def loadMovieNames(): Map[Int, String] = {
    // Handle character encoding issues:
    implicit val codec: Codec = Codec("UTF-8")
    codec onMalformedInput CodingErrorAction.REPLACE
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames: Map[Int, String] = Map()
    val lines = Source.fromFile("ml-100k/u.item").getLines()
    for (line <- lines) {
      var fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    return movieNames
  }
}