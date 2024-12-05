package advent

import org.apache.spark.sql.functions.regexp_extract_all
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}


object Day3Main extends App {
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
    .textFile("data/day3-input.txt")
    .toDF("line")

  df.show()

  private val pattern = """mul\((\d+),(\d+)\)""".r

  private val df2 = df
    .withColumn("number1", regexp_extract_all(df("line"), pattern.toString(), 1).cast(IntegerType))
    .withColumn("number2", regexp_extract_all(df("line"), pattern.toString(), 2).cast(IntegerType))
    .drop("line")

  df2.show()



  // Stop the Spark session
  spark.stop()
}
