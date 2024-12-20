package advent

import org.apache.spark.sql._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

//noinspection DuplicatedCode
object Day13Main extends App {
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
  private case class ArcadeMachine(a: Coord, b: Coord, target: Coord)

  private val df: DataFrame = spark.read
    .textFile("data/day13-input.txt")
    .toDF("lines")

  private val lines = df.collect().map(_.getString(0)).filter(_.nonEmpty)

  val start = System.nanoTime()

  private val machines: ArrayBuffer[ArcadeMachine] = ArrayBuffer()
  private val coordRx = raw"""X[+=](\d+), Y[+=](\d+)""".r
  for(i <- lines.indices by 3) {
    val a = coordRx.findFirstMatchIn(lines(i)).get
    val b = coordRx.findFirstMatchIn(lines(i + 1)).get
    val price = coordRx.findFirstMatchIn(lines(i + 2)).get
    machines.append(ArcadeMachine(
      Coord(a.group(1).toLong, a.group(2).toLong),
      Coord(b.group(1).toLong, b.group(2).toLong),
      Coord(price.group(1).toLong, price.group(2).toLong)
    ))
  }

  private val totalAcc = spark.sparkContext.longAccumulator("totalAcc")
  spark.sparkContext.parallelize(machines.toSeq)
    .repartition(10)
    .foreachPartition(partition => {
      partition.foreach(machine => {
        breakable {
          for(a <- 0 to 100) {
            for(b <- 0 to 100) {
              val pos = Coord(a * machine.a.x + b * machine.b.x, a * machine.a.y + b * machine.b.y)
              if(pos.x == machine.target.x && pos.y == machine.target.y) {
                totalAcc.add(3 * a + b)
                break
              }
            }
          }
        }
      })
    })

  println(s"Total: ${totalAcc.value}")

  private val part2Machines = machines.map(m => ArcadeMachine(m.a, m.b, Coord(m.target.x + 10000000000000L, m.target.y + 10000000000000L)))
  private val results = spark.sparkContext.parallelize(part2Machines.toSeq)
    .repartition(10)
    .mapPartitions(partition => {
      var localSum = 0L
      partition.foreach(m => {
        val denominator: Long = m.a.x * m.b.y - m.a.y * m.b.x
        if (denominator != 0) {
          val p: Double = (m.target.x * m.b.y - m.target.y * m.b.x).toDouble / denominator // Value of a
          val q: Double = (m.a.x * m.target.y - m.a.y * m.target.x).toDouble / denominator // Value of b

          if (p >= 0 && p % 1 == 0 && q >= 0 && q % 1 == 0) {
            localSum += (3 * p.toLong + q.toLong)
          }
        }
      })
      Iterator(localSum) // Returning the local sum for this partition
    }).reduce(_ + _) // Combine results from all partitions

  println(s"Total 2: $results")

  val end = System.nanoTime()
  println(s"Execution time: ${(end - start) / 1000000}ms")

  // Stop the Spark session
  spark.stop()
}
