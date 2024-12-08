package advent

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._


object Day8Main extends App {
  // Create the Spark session
	private val spark = SparkSession.builder()
                          .appName("advent")
                          .config("spark.driver.host", "localhost")
                          .master("local[*]")
                          .getOrCreate()

  // suppress informational log messages related to the inner working of Spark
  spark.sparkContext.setLogLevel("ERROR")

  spark.conf.set("spark.sql.shuffle.partitions", "10")

  private val df: DataFrame = spark.read
    .textFile("data/day8-input.txt")
    .toDF("lines")

  //df.show()

  private val lines = df.collect().map(row => row.getString(0).split(""))
  private val symbols: ArrayBuffer[(String, Int, Int)] = ArrayBuffer() // (symbol, x, y)
  private val pattern = raw"""[A-Za-z0-9]""".r

  for(y <- lines.indices) {
    breakable {
      val line = lines(y)
      val matches = pattern.findAllMatchIn(line.mkString("")).toSeq
      if(matches.isEmpty) break // "continue"
      matches.foreach(m => {
        val x = m.start
        symbols += ((m.toString, x, y))
      })
    }
  }

  // Do some funky trigonometry to find the antinodes
  private val locations: ArrayBuffer[(Int, Int)] = ArrayBuffer()
  private val part2Locations: ArrayBuffer[(Int, Int)] = ArrayBuffer()
  private val uniqueSymbols = symbols.map(_._1).distinct
  uniqueSymbols.foreach(symbol => {
    val symbolLocations = symbols.filter(_._1 == symbol)
    // Calculate possible antinodes for every possible pair of locations
    for(i <- symbolLocations.indices) {
      for(j <- i + 1 until symbolLocations.length) {
        // Get coords
        val (x1, y1) = (symbolLocations(i)._2, symbolLocations(i)._3)
        val (x2, y2) = (symbolLocations(j)._2, symbolLocations(j)._3)
        // Deltas
        val dx = x2 - x1
        val dy = y2 - y1
        // Distance and angle between the two points
        val distance: Double = math.sqrt(dx * dx + dy * dy)
        val angle: Double = math.atan2(dy, dx)

        // Calculate antinodes, each pairing of symbols will have 2 antinodes (one on each side)
        val (ax1, ay1) = (x1 + distance * math.cos(angle + Math.PI), y1 + distance * math.sin(angle  + Math.PI))
        val (ax2, ay2) = (x2 + distance * math.cos(angle), y2 + distance * math.sin(angle))
        locations += ((ax1.round.toInt, ay1.round.toInt))
        locations += ((ax2.round.toInt, ay2.round.toInt))

        // Part 2
        for(d <- 0 until 2 * lines.length) {
          val newDistance: Double = distance * d
          val (ax1, ay1) = (x1 + newDistance * math.cos(angle + Math.PI), y1 + newDistance * math.sin(angle  + Math.PI))
          val (ax2, ay2) = (x2 + newDistance * math.cos(angle), y2 + newDistance * math.sin(angle))
          part2Locations += ((ax1.round.toInt, ay1.round.toInt))
          part2Locations += ((ax2.round.toInt, ay2.round.toInt))
        }
      }
    }
  })

  // Create a df with the locations
  private val locationsDf = createDFFromLocations(locations, spark)
  private val unique = locationsDf.distinct()
    .where(s"x >= 0 AND y >= 0 AND x < ${lines(0).length} AND y < ${lines.length}")
    .count()

  println(s"Unique antinodes: $unique")

  // Part 2
  private val part2DF = createDFFromLocations(part2Locations, spark)
  private val part2Unique = part2DF.distinct()
    .where(s"x >= 0 AND y >= 0 AND x < ${lines(0).length} AND y < ${lines.length}")
    .count()

  println(s"Unique antinodes part 2: $part2Unique")


  private def createDFFromLocations(locations: ArrayBuffer[(Int, Int)], spark: SparkSession): DataFrame = {
    val schema = new StructType()
      .add("x", IntegerType, nullable = false)
      .add("y", IntegerType, nullable = false)
    val rows = locations.map(location => Row(location._1, location._2)).toSeq
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
  }

  // Stop the Spark session
  spark.stop()
}
