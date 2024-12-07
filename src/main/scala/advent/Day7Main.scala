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

  spark.conf.set("spark.sql.shuffle.partitions", "10")

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
    .repartition(10)

  df.show()

  private val part1Acc = spark.sparkContext.longAccumulator("Part 1 Accumulator")
  private val part2Acc = new BigIntAccumulator()
  spark.sparkContext.register(part2Acc, "BigIntAccumulator")


  df.foreachPartition((partition: Iterator[Row]) => {
    partition.foreach(row => {
      val target: BigInt = BigInt(row.getString(0).replace(":", ""))
      val values: Array[Long] = row.toSeq
        .drop(1)
        .filter(_ != null)
        .map(_.toString.toLong)
        .toArray
      breakable {
        var foundPart1 = false
        var foundPart2 = false
        for(multiply <- 0 until Math.pow(2, values.length).toInt) {
          // Part 1
          var part1Total: Long = 0L
          for(x <- values.indices) {
            // If the multiply bit is set, then we multiply the value
            // Otherwise, we add the value
            if((multiply & (1 << x)) != 0) part1Total *= values(x)
            else part1Total += values(x)
          }
          if(part1Total == target && !foundPart1) {
            part1Acc.add(part1Total)
            foundPart1 = true
          }
          // Part 2
          for(concat <- 0 until Math.pow(2, values.length).toInt) {
            var part2Total: BigInt = BigInt(0)
            for(x <- values.indices) {
              // If the multiply bit is set, then we multiply the value
              // If the concat bit is set, then we concatenate the value
              // Otherwise, we add the value
              if((multiply & (1 << x)) != 0) part2Total *= values(x)
              else if((concat & (1 << x)) != 0) part2Total = BigInt(part2Total.toString + values(x).toString)
              else part2Total += values(x)
            }
            if(part2Total == target && !foundPart2) {
              part2Acc.add(part2Total)
              foundPart2 = true
            }
            if(foundPart1 && foundPart2) break
          }
        }
      }
    })
  })

  println(s"Part1 total calibration result: ${part1Acc.value}")
  println(s"Part2 total calibration result: ${part2Acc.value}")


  // Stop the Spark session
  spark.stop()
}
