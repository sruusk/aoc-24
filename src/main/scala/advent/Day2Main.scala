package advent

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, DataFrame, Row, SQLContext, SparkSession, functions}
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{array, asc, avg, col, count, desc, explode, max, min, sum, udf, when, year}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level


object Day2Main extends App {
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


  // Stop the Spark session
  spark.stop()
}
