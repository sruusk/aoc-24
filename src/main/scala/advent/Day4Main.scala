package advent

import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.:+
import scala.collection.mutable.ArrayBuffer


object Day4Main extends App {
  // Create the Spark session
  private val spark = SparkSession.builder()
    .appName("advent")
    .config("spark.driver.host", "localhost")
    .master("local")
    .getOrCreate()

  // suppress informational log messages related to the inner working of Spark
  spark.sparkContext.setLogLevel("ERROR")

  spark.conf.set("spark.sql.shuffle.partitions", "5")

  private val df: DataFrame = spark.read
    .textFile("data/day4-input.txt")
    .toDF("lines")

  private val lines = df.collect().map(row => row.getString(0).split(""))
  private val newLines: ArrayBuffer[String] = ArrayBuffer()

  for(i <- lines.indices) {
    var tlToBrLower = "" // top-left to bottom-right lower diagonal
    var tlToBrUpper = "" // top-left to bottom-right upper diagonal
    var trToBlLower = "" // top-right to bottom-left lower diagonal
    var trToBlUpper = "" // top-right to bottom-left upper diagonal
    var x = 0
    for(y <- i until lines.length) {
      tlToBrLower += lines(y)(x)
      trToBlLower += lines(y)(lines.length - 1 - x)
      if(i > 0) { // Prevent duplicate diagonals
        trToBlUpper += lines(x)(lines.length - 1 - y)
        tlToBrUpper += lines(x)(y)
      }
      x += 1
    }
    newLines += tlToBrLower
    newLines += tlToBrUpper
    newLines += trToBlLower
    newLines += trToBlUpper
  }

  private val pattern = raw"""XMAS""".r
  private val allLines: Array[String] = lines.map(_.mkString("")) ++ newLines ++ lines.transpose.map(_.mkString(""))

  private var occurrences = 0
  allLines.foreach(line => {
    val matches = pattern.findAllMatchIn(line).toSeq ++ pattern.findAllMatchIn(line.reverse).toSeq
    occurrences += matches.size
  })

  println(s"Occurrences: $occurrences")

  // Stop the Spark session
  spark.stop()
}
