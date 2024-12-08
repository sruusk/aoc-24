package advent

import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.Breaks._


object Day6Main extends App {
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
    .textFile("data/day6-input.txt")
    .toDF("lines")

  private val lines = df.collect().map(row => row.getString(0).split(""))
  private val positions: ArrayBuffer[(Int, Int)] = ArrayBuffer()
  private val guard = lines.find(_.contains("^"))



  breakable {
    var direction: String = "U"
    var x = guard.get.indexOf("^")
    var y = lines.indexOf(guard.get)
    while(true) {
      val (newX, newY, newDirection) = getNewCoords(lines, x, y, direction)
      if(isOutOfBounds(newY, newX)) break
      else {
        x = newX; y = newY; direction = newDirection
        positions.append((x, y))
      }
    }
  }

  // Convert the positions to a DataFrame
  private val schema = new StructType()
    .add("x", IntegerType, nullable = true)
    .add("y", IntegerType, nullable = true)
  private val positionsDF = spark.createDataFrame(positions.map { case (x, y) => Row(x, y) }.asJava, schema)

  // Get the distinct positions
  private val distinctPositions = positionsDF.distinct().repartition(10)
  private val totalDistinctPositions = distinctPositions.count() + 1

  println(s"Distinct positions: $totalDistinctPositions")

  // Part 2
  private val possibleObstructions = spark.sparkContext.longAccumulator("Possible Obstructions")

  // Check for possible obstructions
  distinctPositions.foreachPartition((partition: Iterator[Row]) => {
    partition.zipWithIndex.foreach { case (row, i) =>
      val x = row.getInt(0)
      val y = row.getInt(1)
      if (isObstructed(x, y)) possibleObstructions.add(1)
    }
  })

  println(s"Possible obstructions: ${possibleObstructions.value}")

  private def isObstructed(x: Int, y: Int): Boolean = {
    var currentX = guard.get.indexOf("^")
    var currentY = lines.indexOf(guard.get)
    var currentDirection: String = "U"

    val newMap = lines.map(_.clone())
    newMap(y)(x) = "#" // Mark the position as an obstruction

    val turns: ArrayBuffer[(Int, Int, String)] = ArrayBuffer() // Store the turns made
    var infiniteLoop = false
    breakable {
      while (true) {
        val (newX, newY, newDir) = getNewCoords(newMap, currentX, currentY, currentDirection)
        if (isOutOfBounds(newY, newX)) break
        else {
          currentX = newX; currentY = newY; currentDirection = newDir
          // Check if we have done this exact turn before
          if (turns.contains((newX, newY, currentDirection))) {
            infiniteLoop = true
            break
          } else turns.append((newX, newY, currentDirection))
        }
      }
    }
    infiniteLoop
  }

  private def getNewCoords(lines: Array[Array[String]], x: Int, y: Int, direction: String): (Int, Int, String) = {
    var currentDirection = direction
    var (newX, newY) = nextPosition(x, y, currentDirection)
    while (!canMove(newY, newX, lines)) {
      currentDirection match {
        case "U" => currentDirection = "R"
        case "R" => currentDirection = "D"
        case "D" => currentDirection = "L"
        case "L" => currentDirection = "U"
      }
      val (tempX, tempY) = nextPosition(x, y, currentDirection)
      newX = tempX
      newY = tempY
    }
    (newX, newY, currentDirection)
  }

  private def nextPosition(x: Int, y: Int, direction: String): (Int, Int) = {
    direction match {
      case "U" => (x, y - 1)
      case "D" => (x, y + 1)
      case "L" => (x - 1, y)
      case "R" => (x + 1, y)
    }
  }

  private def canMove(newY: Int, newX: Int, lines: Array[Array[String]]): Boolean = {
    if(isOutOfBounds(newY, newX)) true
    else lines(newY)(newX) != "#"
  }

  private def isOutOfBounds(newY: Int, newX: Int): Boolean = {
    newY < 0 || newY >= lines.length || newX < 0 || newX >= lines(0).length
  }

  // Stop the Spark session
  spark.stop()
}
