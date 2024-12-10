package advent

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

object Day10Main extends App {
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
    .textFile("data/day10-input.txt")
    .toDF("lines")
    .withColumn("row", monotonically_increasing_id())

  // Measure execution time
  val start = System.nanoTime()

  private val trailHeadRx = raw"""(0)""".r
  private val trailHeads: RDD[(Int, Int)] = df.rdd.flatMap((r: Row) => {
    val row = r.getLong(1).toInt
    val line = r.getString(0)

    val heads = trailHeadRx.findAllMatchIn(line).toSeq.map(_.start)
    if (heads.nonEmpty) {
      heads.map(h => Tuple2(h, row))
    } else {
      Seq()
    }
  }).repartition(10)

  private val map: Array[Array[Int]] = df.collect().map(_.getString(0).split("").map(_.toInt))
  private val trailScoreAcc = spark.sparkContext.longAccumulator("trailScore")
  private val distinctTrailScoreAcc = spark.sparkContext.longAccumulator("distinctTrailScore")

  trailHeads.foreachPartition((it: Iterator[(Int, Int)]) => {
    it.foreach((t: (Int, Int)) => {
      val startX = t._1
      val startY = t._2
      val v: Int = map(startY)(startX)
      val mapWidth: Int = map(0).length
      val mapHeight: Int = map.length
      val trailHeadsReached: ArrayBuffer[(Int, Int)] = ArrayBuffer() // For Part 1
      val distinctTrails: ArrayBuffer[ArrayBuffer[(Int, Int)]] = ArrayBuffer(ArrayBuffer()) // For Part 2
      if(v != 0) throw new Exception("Invalid trail head")
      var nextId = 0

      // x, y, prev value, trail index
      def getTrailScore(x: Int, y: Int, prev: Int, trail: Int): Int = {
        var s = 0
        val curr = map(y)(x)
        if(curr - prev == 1) {
          distinctTrails(trail) += Tuple2(x, y)
          if(curr == 9) {
            if(!trailHeadsReached.contains((x, y))) {
              s = 1
              trailHeadsReached.append((x, y))
            }
          } else {
            Seq((x+1, y), (x, y-1), (x-1, y), (x, y+1)).foreach((dir: (Int, Int)) => {
              if(dir._1 >= 0 && dir._1 < mapWidth && dir._2 >= 0 && dir._2 < mapHeight) {
                distinctTrails += distinctTrails(trail).clone()
                nextId += 1
                s += getTrailScore(dir._1, dir._2, curr, nextId)
              }
            })
          }
        }
        s
      }
      val score: Int = getTrailScore(startX, startY, -1, 0)
      trailScoreAcc.add(score)
      val distinct = distinctTrails.filter(_.nonEmpty).filter(t => map(t.last._2)(t.last._1) == 9).distinctBy(_.toString)
      distinctTrailScoreAcc.add(distinct.length)
    })
  })

  println(s"Total trail scores: ${trailScoreAcc.sum}")
  println(s"Distinct trail scores: ${distinctTrailScoreAcc.sum}")

  val end = System.nanoTime()
  println(s"Execution time: ${(end - start) / 1000000}ms")

  // Stop the Spark session
  spark.stop()
}
