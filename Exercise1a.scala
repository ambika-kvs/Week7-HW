package Week7

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
object Exercise1a {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    val session = SparkSession
      .builder
      .master("local[2]")
      .appName("word_1")
      .getOrCreate()
    val input = session.read.textFile("TheHungerGames.txt")
    import session.implicits._
    val words = input.flatMap(x => x.toLowerCase().split("[^\\w]")).filter(x => x.nonEmpty && x.length >= 3)
//    val wordCounts = words.rdd.countByValue().toList.sorted.reverse
    val wordCounts = words.rdd.countByValue().toList.sortBy(_._2).reverse
    wordCounts.foreach(println)
    println()
    println()
    println("#######################")
    println("Top Ten Words")
    println("#######################")
    wordCounts.take(10).foreach(println)



    val top10 = wordCounts.take(10)
    println()
    println()
    println("##################################")
    println("Top Ten Words in Ascending order")
    println("##################################")
    top10.sortBy(x => x._1).foreach(println(_))

  }
}