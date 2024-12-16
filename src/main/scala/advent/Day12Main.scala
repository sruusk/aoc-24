package advent

import org.apache.commons.math3.geometry.Point
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{abs, col, concat_ws, monotonically_increasing_id}
import org.apache.spark.sql._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

object Day12Main extends App {
  // Create the Spark session
	private val spark = SparkSession.builder()
                          .appName("advent")
                          .config("spark.driver.host", "localhost")
                          .master("local[*]")
                          .getOrCreate()

  // suppress informational log messages related to the inner working of Spark
  spark.sparkContext.setLogLevel("ERROR")

  spark.conf.set("spark.sql.shuffle.partitions", "10")

  private case class Location(x: Int, y: Int, state: String)

  private val df: DataFrame = spark.read
    .textFile("data/day12-test-input.txt")
    .toDF("lines")
    .withColumn("row", monotonically_increasing_id())

  val start = System.nanoTime()

  private val gardens: Array[Location] = df.rdd.flatMap((r: Row) => {
    val row = r.getLong(1).toInt
    val line = r.getString(0).split("")
    for(i <- line.indices) yield Location(i, row, line(i))
  }).collect()

  private var visitedLocations: Set[Location] = Set.empty
  private val groups: ArrayBuffer[Set[Location]] = ArrayBuffer.empty

  gardens.foreach(l => {
    if(!visitedLocations.contains(l)) {
      groups += traverseGarden(l)
    }
  })

  private val total: Long = groups
    .map(l => (l.size, getPerimeter(l)))
    .map(t => t._1 * t._2)
    .sum

  println(s"Total: $total")


  // Part 2


  private def getPerimeter(group: Set[Location]): Int = {
    group.toArray.map(l => 4 - getNeighbours(l).length).sum
  }

  private def traverseGarden(location: Location): Set[Location] = {
    if(visitedLocations.contains(location)) Set.empty
    else {
      visitedLocations += location
      val neighbours: Array[Location] = getNeighbours(location).filter(l => !visitedLocations.contains(l))
      val group: Set[Location] = neighbours.flatMap(traverseGarden).toSet
      group + location
    }
  }

  private def getNeighbours(location: Location): Array[Location] = {
    gardens.filter(l => {
      l.state == location.state &&
      ((l.x == location.x && l.y == location.y - 1) ||
        (l.x == location.x && l.y == location.y + 1) ||
        (l.x == location.x - 1 && l.y == location.y) ||
        (l.x == location.x + 1 && l.y == location.y))
    })
  }

  val end = System.nanoTime()
  println(s"Execution time: ${(end - start) / 1000000}ms")

  // Stop the Spark session
  spark.stop()
}
