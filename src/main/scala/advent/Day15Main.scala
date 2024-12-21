package advent

import org.apache.spark.sql._

import scala.collection.mutable.ArrayBuffer

//noinspection DuplicatedCode
object Day15Main extends App {
  // Create the Spark session
	private val spark = SparkSession.builder()
                          .appName("advent")
                          .config("spark.driver.host", "localhost")
                          .master("local[*]")
                          .getOrCreate()

  // suppress informational log messages related to the inner working of Spark
  spark.sparkContext.setLogLevel("ERROR")

  spark.conf.set("spark.sql.shuffle.partitions", "10")

  case class Object(var pos: (Int, Int), width: Int, kind: Char)
  private case class Direction(dx: Int, dy: Int)
  private val directions = Map(
    '^' -> Direction(0, -1),
    'v' -> Direction(0, 1),
    '<' -> Direction(-1, 0),
    '>' -> Direction(1, 0)
  )

  private val df: DataFrame = spark.read
    .textFile("data/day15-test-input.txt")
    .toDF("lines")

  private val lines = df.collect().map(_.getString(0))

  val start = System.nanoTime()

  private var map: ArrayBuffer[Object] = ArrayBuffer.empty
  private val moves: ArrayBuffer[Direction] = ArrayBuffer.empty

  private var mapped = false
  private var i = 0
  lines.foreach(line => {
    if(line.isEmpty) {
      mapped = true
    }
    else if(mapped) {
      moves ++= line.map(c => directions(c))
    } else {
      map ++= line.toCharArray.zipWithIndex.map(t => Object((t._2, i), 1, t._1)).filter(_.kind != '.')
    }
    i += 1
  })

  val part2Map = map.clone().map(o => {
    val newWidth = if(o.kind == '@') 1 else 2
    Object(
      (o.pos._1 * 2, o.pos._2),
      newWidth,
      o.kind
    )
  })

//  printMap()
//  println(s"Robot position: $getRobotPos")

  moves.foreach(m => { move(getRobotPos, m) })

  private val total = map.filter(_.kind == 'O').map(a => 100 * a.pos._2 + a.pos._1).sum
  println(s"Total: $total")

  // Part 2
  map = part2Map
  printMap()

  moves.foreach(m => {
    println(s"Move: $m")
    move(getRobotPos, m)
    printMap()
  })

  private val total2 = map.filter(_.kind == 'O').map(a => 100 * a.pos._2 + a.pos._1).sum
  println(s"Total: $total2")

  private def isBox(pos: (Int, Int)): Boolean = {
    map.exists(o => o.pos._2 == pos._2 && o.pos._1 <= pos._1 && pos._1 < o.pos._1 + o.width && o.kind == 'O')
  }

  private def isWall(pos: (Int, Int)): Boolean = {
    map.exists(o => o.pos._2 == pos._2 && o.pos._1 <= pos._1 && pos._1 < o.pos._1 + o.width && o.kind == '#')
  }

  private def isFree(pos: (Int, Int)): Boolean = {
    !isWall(pos) && !isBox(pos)
  }

  private def isRobot(pos: (Int, Int)): Boolean = {
    map.exists(o => o.pos._2 == pos._2 && o.pos._1 <= pos._1 && pos._1 < o.pos._1 + o.width && o.kind == '@')
  }

  private def setNewPos(pos: (Int, Int), newPos: (Int, Int)): Unit = {
    map.find(_.pos == pos).get.pos = newPos
  }

  private def getObjectAtPost(pos: (Int, Int)): Object = {
    map.find(o => o.pos._2 == pos._2 && o.pos._1 <= pos._1 && pos._1 < o.pos._1 + o.width).get
  }

  private def getRobotPos: (Int, Int) = {
    map.find(_.kind == '@').get.pos
  }

  private def move(pos: (Int, Int), direction: Direction): Boolean = {
    val obj = getObjectAtPost(pos)
    val newPos = (obj.pos._1 + direction.dx, obj.pos._2 + direction.dy)
    val newPositions: Seq[(Int, Int)] = Math.abs(direction.dy) match {
      case 1 => {
        (0 until obj.width).map(i => (obj.pos._1 + i + direction.dx, obj.pos._2 + direction.dy))
      }
      case _ => {
        Seq(newPos)
      }
    }

    if(newPositions.forall(isFree)) { // If all new positions are free
      setNewPos(pos, newPos)
      true
    }
    else if(newPositions.exists(isBox)) {
      val boxes = newPositions.filter(isBox).map(p => getObjectAtPost(p)).toSet
      if(boxes.forall(b => move(b.pos, direction))) {
        setNewPos(pos, newPos)
        true
      }
      else false
    } else {
      false
    }
  }

  private def printMap(): Unit = {
    val maxX = map.map(_.pos._1).max
    val maxY = map.map(_.pos._2).max

    for(y <- 0 to maxY) {
      for(x <- 0 to maxX) {
        val pos = (x, y)
        if(isRobot(pos)) {
          print('@')
        } else if(isBox(pos)) {
          print('O')
        } else if(isWall(pos)) {
          print('#')
        } else {
          print('.')
        }
      }
      println()
    }
  }

  val end = System.nanoTime()
  println(s"Execution time: ${(end - start) / 1000000}ms")

  // Stop the Spark session
  spark.stop()
}
