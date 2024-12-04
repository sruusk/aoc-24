package advent

import advent.Day2Main.{df, spark}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, Row, SQLContext, SparkSession, functions}
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{array, asc, avg, col, count, desc, explode, max, min, sum, udf, year}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level


object Day1Main extends App {
  // Create the Spark session
	private val spark = SparkSession.builder()
                          .appName("advent")
                          .config("spark.driver.host", "localhost")
                          .master("local")
                          .getOrCreate()

  // suppress informational log messages related to the inner working of Spark
  spark.sparkContext.setLogLevel("ERROR")

  spark.conf.set("spark.sql.shuffle.partitions", "5")

  val schema = new StructType()
    .add("number1", IntegerType, nullable = true)
    .add("number2", IntegerType, nullable = true)

  private val df: DataFrame = spark.read
    .option("delimiter", "   ")
    .schema(schema)
    .csv("data/input.txt")


  // Sort the two columns separately
  private val sorted1 = df.select("number1").sort(asc("number1"))
  private val sorted2 = df.select("number2").sort(asc("number2"))

  // Join the two dataframes
  private val sortedDF: DataFrame = spark.createDataFrame(sorted1.rdd.zip(sorted2.rdd).map {
    case (rowLeft, rowRight) => Row.fromSeq(rowLeft.toSeq ++ rowRight.toSeq)
  }, schema)

  private var totalDistance = sortedDF.groupBy("number1", "number2")
    .agg(functions.abs(col("number1") - col("number2")).as("distance"))
    .agg(sum("distance").as("total_distance"))
    .select("total_distance")
    .first()
    .get(0)

  println(s"Total distance: $totalDistance")


  private val counts = spark.sparkContext.longAccumulator("counts")

  // Count the number of occurrences of each number in the number2 column
  val num2Occurrences = df.groupBy("number2").count().collect().map(row => (row.getInt(0), row.getLong(1))).toMap

  df.foreach(row => {
    val number1 = row.getInt(0)
    val occurrences = num2Occurrences.getOrElse(number1, 0L)
    if(occurrences > 0L) {
      counts.add(occurrences * number1)
    }
  })

  println(s"Similarity score: ${counts.sum}")

  // Stop the Spark session
  spark.stop()
}
