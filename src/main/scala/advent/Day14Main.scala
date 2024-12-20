package advent

import org.apache.spark.sql._

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

//noinspection DuplicatedCode
object Day14Main extends App {
  // Create the Spark session
	private val spark = SparkSession.builder()
                          .appName("advent")
                          .config("spark.driver.host", "localhost")
                          .master("local[*]")
                          .getOrCreate()

  // suppress informational log messages related to the inner working of Spark
  spark.sparkContext.setLogLevel("ERROR")

  spark.conf.set("spark.sql.shuffle.partitions", "10")

  private case class Coord(x: Long, y: Long)
  private case class Robot(pos: Coord, velocity: Coord)

  private val df: DataFrame = spark.read
    .textFile("data/day14-input.txt")
    .toDF("lines")

  private val lines = df.collect().map(_.getString(0)).filter(_.nonEmpty)

  val start = System.nanoTime()

  private val coordRx = raw"""p=(-?\d+),(-?\d+) v=(-?\d+),(-?\d+)""".r
  private var robots: Array[Robot] = lines.map(line => {
    val coordRx(posX, posY, velX, velY) = line
    Robot(Coord(posX.toLong, posY.toLong), Coord(velX.toLong, velY.toLong))
  })
  private var part2Robots = robots.clone()

//  private val roomDimensions = Coord(11, 7)
  private val roomDimensions = Coord(101, 103)
  private val seconds = 100

  for(i <- 0 until seconds) {
    robots = robots.map(moveRobot)
  }

  private val cx = roomDimensions.x / 2
  private val cy = roomDimensions.y / 2
  private val quadrants = robots.filter(r => r.pos.x != cx && r.pos.y != cy)
    .groupBy(r => {
      if(r.pos.x < cx && r.pos.y < cy) 1
      else if(r.pos.x > cx && r.pos.y < cy) 2
      else if(r.pos.x < cx && r.pos.y > cy) 3
      else 4
    })

  private var safetyFactor: Int = 1
  quadrants.map(_._2.length).foreach(safetyFactor *= _)
  println(s"Safety factor: $safetyFactor")

  // Part 2
  var i = 0L
  while(!isShape) {
    part2Robots = part2Robots.map(moveRobot)
    i += 1
    print(s"Seconds: $i\r")
  }

  println(s"\nPart 2:")
  for(y <- 0 until roomDimensions.y.toInt) {
    for(x <- 0 until roomDimensions.x.toInt) {
      if(part2Robots.exists(r => r.pos.x == x && r.pos.y == y)) {
        print("#")
      }
      else {
        print(".")
      }
    }
    println()
  }


  val end = System.nanoTime()
  println(s"Execution time: ${(end - start) / 1000000}ms")

  private def moveRobot(robot: Robot): Robot = {
    var newX = robot.pos.x + robot.velocity.x
    var newY = robot.pos.y + robot.velocity.y
    if(newX >= roomDimensions.x) {
      newX -= roomDimensions.x
    }
    else if (newX < 0) {
      newX += roomDimensions.x
    }
    if(newY >= roomDimensions.y) {
      newY -= roomDimensions.y
    }
    else if (newY < 0) {
      newY += roomDimensions.y
    }
    Robot(Coord(newX, newY), robot.velocity)
  }

  // Find any shape consisting of at least 10 adjacent robots
  private def isShape: Boolean = {
    var visited: Set[Coord] = Set.empty

    def traverse(coord: Coord): Set[Coord] = {
      if (visited.contains(coord)) Set.empty
      else {
        visited += coord
        val neighbours = getNeighbours(coord).filterNot(visited.contains)
        val group = neighbours.flatMap(traverse).toSet
        group + coord
      }
    }

    def getNeighbours(coord: Coord): Array[Coord] = {
      part2Robots.filter(r => {
        val pos = r.pos
        (pos.x == coord.x && (pos.y == coord.y - 1 || pos.y == coord.y + 1)) ||
          (pos.y == coord.y && (pos.x == coord.x - 1 || pos.x == coord.x + 1))
      }).map(_.pos)
    }

    part2Robots.exists(r => traverse(r.pos).size >= 20)
  }

  // Stop the Spark session
  spark.stop()
}
