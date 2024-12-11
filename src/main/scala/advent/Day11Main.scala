package advent

import org.apache.spark.sql.functions.{asc, col, sum}
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Day11Main extends App {
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
    .textFile("data/day11-input.txt")
    .toDF("lines")

  // Measure execution time
  val start = System.nanoTime()

  private case class Stone(_1: Long, _2: String)

  private val inputStones: Array[Stone] = df.collect()(0)
    .getString(0)
    .split(" ")
    .map(s => Stone(s.toLong, s))

  @tailrec
  private def processStones(stones: ArrayBuffer[Stone], blinks: Int): ArrayBuffer[Stone] = {
    if(blinks <= 0) stones
    else processStones(stones.flatMap(processStone), blinks - 1)
  }

  private def processStone(stone: Stone): Array[Stone]  = {
    if(stone._1 == 0) Array(Stone(1, "1"))
    else if(stone._2.length % 2 == 0) {
      val (left: String, right: String) = stone._2.splitAt(stone._2.length / 2)
      Array(Stone(left.toLong, left.toLong.toString), Stone(right.toLong, right.toLong.toString))
    }
    else {
      val newStone = 2024 * stone._1
      Array(Stone(newStone, newStone.toString))
    }
  }

  /*
    Processes the stones recursively starting from the first stone on each level
    until the number of blinks is reached
    Returns the number of stones when it reaches the target number of blinks
  */
  private val cache = mutable.Map.empty[String, Long]
  private def processStoneRecursively(stone: Stone, blinks: Int, stepSize: Int): Long = {
    val newStones = processStones(ArrayBuffer(stone), stepSize)
    if(blinks <= stepSize) {
      newStones.length
    }
    else {
      var sum: Long = 0L
      for(stone <- newStones) {
        sum += cache.getOrElseUpdate(stone._2 + "-" + blinks.toString, processStoneRecursively(stone, blinks - stepSize, stepSize))
      }
      sum
    }
  }

  private def processStonesInBatches(targetBlinks: Int): Long = {
    val stones = inputStones
    var result: Long = 0
    val stepSize = 5
    for(stone <- stones) {
      result += processStoneRecursively(stone, targetBlinks, stepSize)
    }
    result
  }

  println(s"Part1 Stones: ${processStonesInBatches(25)}")
  println(s"Part2 Stones: ${processStonesInBatches(75)}")

  val end = System.nanoTime()
  println(s"Execution time: ${(end - start) / 1000000}ms")

  // Stop the Spark session
  spark.stop()
}
