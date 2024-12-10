package advent

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.util.control.Breaks._


object Day7Main extends App {
  // Create the Spark session
	private val spark = SparkSession.builder()
                          .appName("advent")
                          .config("spark.driver.host", "localhost")
                          .master("local[*]")
                          .getOrCreate()

  // suppress informational log messages related to the inner working of Spark
  spark.sparkContext.setLogLevel("ERROR")

  spark.conf.set("spark.sql.shuffle.partitions", "100")

  // Measure the time taken to execute the code
  val start = System.nanoTime()

  val schema = new StructType() // This is stupid
    .add("target", StringType)
    .add("value1", IntegerType)
    .add("value2", IntegerType)
    .add("value3", IntegerType)
    .add("value4", IntegerType)
    .add("value5", IntegerType)
    .add("value6", IntegerType)
    .add("value7", IntegerType)
    .add("value8", IntegerType)
    .add("value9", IntegerType)
    .add("value10", IntegerType)
    .add("value11", IntegerType)
    .add("value12", IntegerType)
    .add("value13", IntegerType)
    .add("value14", IntegerType)
    .add("value15", IntegerType)
    .add("value16", IntegerType)
    .add("value17", IntegerType)
    .add("value18", IntegerType)
    .add("value19", IntegerType)
    .add("value20", IntegerType)

  private val df: DataFrame = spark.read
    .option("delimiter", " ")
    .schema(schema)
    .csv("data/day7-input.txt")
    .repartition(100)

  private val part1Acc = spark.sparkContext.longAccumulator("Part 1 Accumulator")
  private val part2Acc = spark.sparkContext.longAccumulator("Part 2 Accumulator")
//  private val part2Acc = new BigIntAccumulator()
//  spark.sparkContext.register(part2Acc, "BigIntAccumulator")


  df.foreachPartition((partition: Iterator[Row]) => {
    partition.foreach(row => {
      val target: Long = row.getString(0).replace(":", "").toLong
      val values: Array[Long] = row.toSeq
        .drop(1)
        .filter(_ != null)
        .map(_.toString.toLong)
        .toArray
      runRow(target, values)
    })
  })

  private def runRow(target: Long, values: Array[Long]): Unit = {
    val power = Math.pow(2, values.length).toInt
    val stringValues = values.map(_.toString)
    var foundPart2: Boolean = false
    var part1Total: Long = 0L
    var part2Total: Long = 0L

    def runPart2(multiply: Int): Unit = {
      if (foundPart2) return
      for (concat <- 0 until power) {
        part2Total = 0L
        breakable {
          for (x <- values.indices) {
            // If the multiply bit is set, then we multiply the value
            // If the concat bit is set, then we concatenate the value
            // Otherwise, we add the value
            if ((multiply & (1 << x)) != 0) part2Total *= values(x)
            else if ((concat & (1 << x)) != 0) part2Total = part2Total * Math.pow(10, stringValues(x).length).toLong + values(x)
            else part2Total += values(x)
            if(part2Total > target) break
          }
        }
        if (part2Total == target) {
          part2Acc.add(part2Total)
          foundPart2 = true
          return
        }
      }
    }

    for (multiply <- 0 until power) {
      // Part 1
      part1Total = 0L
      for (x <- values.indices) {
        // If the multiply bit is set, then we multiply the value
        // Otherwise, we add the value
        if ((multiply & (1 << x)) != 0) part1Total *= values(x)
        else part1Total += values(x)
      }
      if (part1Total == target) {
        part1Acc.add(target)
        if (!foundPart2) part2Acc.add(target)
        return
      }
      // Part 2
      runPart2(multiply)
    }
  }

  println(s"Part1 total calibration result: ${part1Acc.value}")
  println(s"Part2 total calibration result: ${part2Acc.value}")

  val end = System.nanoTime()
  println(s"Time taken: ${(end - start) / 1000000}ms")


  // Stop the Spark session
  spark.stop()
}
