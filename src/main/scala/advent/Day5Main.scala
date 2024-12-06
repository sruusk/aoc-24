package advent

import org.apache.spark.sql.functions.{asc, col, sum}
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}


object Day5Main extends App {
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
    .add("page1", IntegerType, nullable = true)
    .add("page2", IntegerType, nullable = true)

  private val rulesDF: DataFrame = spark.read
    .option("delimiter", "|")
    .schema(schema)
    .csv("data/day5.1-input.txt")

  private val updatesDF = spark.read
    .textFile("data/day5.2-input.txt")
    .toDF("updates")

  private val allRules = rulesDF.collect()
  private val pageNumberAcc = spark.sparkContext.longAccumulator("Page Number Accumulator")
  private val part2Acc = spark.sparkContext.longAccumulator("Part 2 Accumulator")

  updatesDF.foreach(update => {
    val updateArray = update.getString(0).split(",").map(_.toInt)
    var isValid = allRules.forall(rule => testRule(rule, updateArray))
    if(isValid) pageNumberAcc.add(updateArray(updateArray.length / 2))
    else {
      while(!isValid) { // Potential infinite loop :)
        // Find the first rule that fails
        val breakingRule = allRules.find(rule => !testRule(rule, updateArray))
        // Flip the page numbers
        val page1 = breakingRule.get.getInt(0)
        val page2 = breakingRule.get.getInt(1)
        val page1Index = updateArray.indexOf(page1)
        val page2Index = updateArray.indexOf(page2)
        updateArray.update(page1Index, page2)
        updateArray.update(page2Index, page1)
        isValid = allRules.forall(rule => testRule(rule, updateArray))
      }
      part2Acc.add(updateArray(updateArray.length / 2))
    }
  })

  println(s"Acc Page numbers: ${pageNumberAcc.sum}")
  println(s"Acc Part 2: ${part2Acc.sum}")

  private def testRule(rule: Row, update: Array[Int]): Boolean = {
    val page1 = rule.getInt(0)
    val page2 = rule.getInt(1)
    val page1Index = update.indexOf(page1)
    val page2Index = update.indexOf(page2)
    if(page1Index == -1 || page2Index == -1) true
    else if(page1Index < page2Index) true
    else false
  }


  // Stop the Spark session
  spark.stop()
}
