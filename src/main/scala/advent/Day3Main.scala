package advent
import advent.Day2Main.spark
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

  private val df: DataFrame = spark.read
    .textFile("data/day3-input.txt")
    .toDF("line")

  private val pattern = raw"""mul\((\d+),(\d+)\)|(do(?:n't)?(?=\(\)))""".r

  private val multiples = spark.sparkContext.longAccumulator("multiples")
  private val enabledMultiples = spark.sparkContext.longAccumulator("enabledMultiples")

  private var enabled: Boolean = true

  df.foreach(row => {
    val line = row.getString(0)
    val matches = pattern.findAllMatchIn(line)
    matches.foreach(m => {
      val action = m.group(3)
      if(action != null) {
        enabled = action == "do"
      } else {
        val number1 = m.group(1).toLong
        val number2 = m.group(2).toLong
        multiples.add(number1 * number2)
        if(enabled) enabledMultiples.add(number1 * number2)
      }
    })
  })

  println(s"Multiples: ${multiples.value}")
  println(s"Enabled multiples: ${enabledMultiples.value}")



  // Stop the Spark session
  spark.stop()
}
