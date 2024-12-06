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
  private val tlDiagonals: ArrayBuffer[String] = ArrayBuffer()
  private val trDiagonals: ArrayBuffer[String] = ArrayBuffer()

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
    tlDiagonals += tlToBrLower
    if(tlToBrUpper.nonEmpty) tlDiagonals.prepend(tlToBrUpper)
    trDiagonals += trToBlLower
    if(trToBlUpper.nonEmpty) trDiagonals.prepend(trToBlUpper)
  }

  private val pattern = raw"""XMAS""".r
  private val allLines: Array[String] =
    lines.map(_.mkString("")) ++ lines.transpose.map(_.mkString("")) ++ tlDiagonals ++ trDiagonals

  private var occurrences = 0
  allLines.foreach(line => {
    val matches = pattern.findAllMatchIn(line).toSeq ++ pattern.findAllMatchIn(line.reverse).toSeq
    occurrences += matches.size
  })

  println(s"XMAS: $occurrences")


  // Part 2
  private val centerIndex = tlDiagonals.length / 2
  private val xmasPattern = raw"""MAS""".r
  private val samxPattern = raw"""SAM""".r
  private var occurrences2 = 0
  for(i <- tlDiagonals.indices) {
    // First find indexes of all occurrences of X-MAS in the diagonals
    val tlMatches = xmasPattern.findAllMatchIn(tlDiagonals(i)).toSeq ++ samxPattern.findAllMatchIn(tlDiagonals(i)).toSeq
    tlMatches.foreach(m => {
      // Get the center of the match.
      val center = m.start + 1
      // Calculate the index of the match in trDiagonals
      val trIndex = Math.abs(centerIndex - i) + (2 * center)
      // Get the matches in trDiagonals
      val trMatches = xmasPattern.findAllMatchIn(trDiagonals(trIndex)).toSeq ++ samxPattern.findAllMatchIn(trDiagonals(trIndex)).toSeq
      // Compare the indexes of the matches
      trMatches.foreach(m2 => {
        val center2 = m2.start + 1
        val tlIndex = Math.abs(centerIndex - trIndex) + (2 * center2)
        if(tlIndex == i) occurrences2 += 1
      })
    })
  }

  println(s"X-MAS: $occurrences2")

  // Stop the Spark session
  spark.stop()
}
