package Week7

import org.apache.spark._
import org.apache.log4j.{Level, Logger}

import java.io.FileNotFoundException
import java.nio.charset.CodingErrorAction
import scala.io.{Codec, Source}
import scala.util.Failure

object Exercise2 {
  /** Our main function where the action happens */
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val conf = new  SparkConf().setMaster("local[*]").setAppName("PopularMovies").set("spark.driver.host", "localhost");
    // Create a SparkContext using every core of the local machine, named PopularMovies
    //alternative: val sc = new SparkContext("local[*]", "PopularMovies")
    val sc = new SparkContext(conf)
    // Read in each rating line
    val lines = sc.textFile("ml-100k/u.data")

    val movieRating = lines.map(oi => (oi.split("\t")(1).toInt, (oi.split("\t")(2).toFloat,1)))
    val avgRatingPerMovieId = movieRating.reduceByKey((x, y) => (x._1+y._1,x._2+y._2)).
      map(i=>(i._1,i._2._1/i._2._2))

    // Flip (movieID, averageRating) to (averageRating, movieID)
    val flipped = avgRatingPerMovieId.map( x => (x._2, x._1) )

    // Sort
    val sortedMovies = flipped.sortByKey()
    // Collect and print results
    val results = sortedMovies.collect()
    results.foreach(println)

    // Create a broadcast variable of our ID -> movie name map
    var nameDict = sc.broadcast(loadMovieNames())

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
