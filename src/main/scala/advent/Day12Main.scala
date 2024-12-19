package advent

import org.apache.commons.math3.geometry.Point
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{abs, col, concat_ws, monotonically_increasing_id}
import org.apache.spark.sql._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

//noinspection DuplicatedCode
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
    .textFile("data/day12-input.txt")
    .toDF("lines")

  val start = System.nanoTime()

  private var n = 0
  private val gardens: Array[Location] = df.collect().flatMap((r: Row) => {
    val row = n
    n += 1
    val line = r.getString(0).split("")
    for(i <- line.indices) yield Location(i, row, line(i))
  })

  private val neighbourCache = mutable.Map.empty[Location, Array[Location]]
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
  private val cornerDirections = Array(
    (-1, 0, -1, -1, 0, -1), // top left
    (1, 0, 1, -1, 0, -1), // top right
    (-1, 0, -1, 1, 0, 1), // bottom left
    (1, 0, 1, 1, 0, 1) // bottom right
  )
  private val total2: Int = groups.map(g => {
    var corners: Int = 0
    for(loc <- g) {
      for(dir <- cornerDirections) {
        val left = Location(loc.x + dir._1, loc.y + dir._2, loc.state)
        val center = Location(loc.x + dir._3, loc.y + dir._4, loc.state)
        val right = Location(loc.x + dir._5, loc.y + dir._6, loc.state)
        if((isOutside(left) || !g.exists(l => l.x == left.x && l.y == left.y)) &&
          (isOutside(right) || !g.exists(l => l.x == right.x && l.y == right.y))) {
          corners += 1
        } else if((!isOutside(left) && g.exists(l => l.x == left.x && l.y == left.y)) &&
          (isOutside(center) || !g.exists(l => l.x == center.x && l.y == center.y)) &&
          (!isOutside(right) && g.exists(l => l.x == right.x && l.y == right.y)))
        {
          corners += 1
        }
      }
    }
    corners * g.size
  }).sum

  println(s"Total2: $total2")


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
    neighbourCache.getOrElseUpdate(location, {
      gardens.filter(l => {
        l.state == location.state &&
          ((l.x == location.x && l.y == location.y - 1) ||
            (l.x == location.x && l.y == location.y + 1) ||
            (l.x == location.x - 1 && l.y == location.y) ||
            (l.x == location.x + 1 && l.y == location.y))
      })
    })
  }

  private def isOutside(location: Location): Boolean = {
    location.x < 0 || location.y < 0 || location.x >= gardens.length || location.y >= gardens.length
  }

  val end = System.nanoTime()
  println(s"Execution time: ${(end - start) / 1000000}ms")

  // Stop the Spark session
  spark.stop()
}
