package advent

import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


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
    .add("number3", IntegerType, nullable = true)
    .add("number4", IntegerType, nullable = true)
    .add("number5", IntegerType, nullable = true)
    .add("number6", IntegerType, nullable = true)
    .add("number7", IntegerType, nullable = true)
    .add("number8", IntegerType, nullable = true)


  private val df: DataFrame = spark.read
    .option("delimiter", " ")
    .schema(schema)
    .csv("data/day2-input.txt")

  private val safeCount = spark.sparkContext.longAccumulator("safe")

  df.foreach(row => {
    if(checkRow(row)) {
      safeCount.add(1)
    }
  })

  println(s"Safe count: ${safeCount.value}")

  private val safeDamperCount = spark.sparkContext.longAccumulator("safeDamper")

  df.foreach(row => {
    var safe = false

    // Check if the original row is safe
    if(checkRow(row)) safe = true

    // Check if the row is safe with one number removed
    for(i <- 0 until row.toSeq.count(_ != null)) {
      // Remove the number at index i
      val newRow = Row.fromSeq(row.toSeq.updated(i, null).filter(_ != null))
      if(!safe && checkRow(newRow)) {
        safe = true
      }
    }
    if(safe) safeDamperCount.add(1)
  })

  println(s"Safe count with damper: ${safeDamperCount.value}")

  def checkRow(row: Row): Boolean = {
    var safe = true
    val rowSeq = row.toSeq.filter(_ != null)
    val sortedDec = rowSeq.map(_.asInstanceOf[Int]).sorted(Ordering[Int].reverse)
    val sortedInc = rowSeq.map(_.asInstanceOf[Int]).sorted(Ordering[Int])
    if(rowSeq != sortedInc && rowSeq != sortedDec) {
      safe = false
    }

    // Check that the difference between the two numbers is greater than 1 and less than 4
    for(i <- 0 until rowSeq.length - 1) {
      try {
        val distance = math.abs(row.getInt(i) - row.getInt(i + 1))
        if(distance < 1 || distance > 3) {
          safe = false
        }
      } catch {
        case e: NullPointerException => ; // Do nothing, these exceptions are expected
        case e: Exception => println(e)
      }
    }
    safe
  }

  // Stop the Spark session
  spark.stop()
}
