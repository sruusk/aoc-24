package advent

import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._


object Day9Main extends App {
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
    .textFile("data/day9-input.txt")
    .toDF("lines")

  //df.show()

  private val rx = raw"""((\d)(\d)|\d$$)""".r
  private val disk = df.collect()(0).getString(0)

  private val diskSectors: Array[(Int, Int)] = rx.findAllIn(disk).toSeq.map(s => {
    if(s.length > 1) (s(0).asDigit, s(1).asDigit)
    else (s(0).asDigit, -1)
  }).toArray
  private val diskBlocks: ArrayBuffer[String] = ArrayBuffer() // Index or "." for free blocks

  // Read disk sectors to blocks
  for(i <- diskSectors.indices) {
    val sector = diskSectors(i)
    diskBlocks ++= ((i.toString + "-") * sector._1).split("-")
    if(sector._2 > 0) {
      diskBlocks ++= ("." * sector._2).split("")
    }
  }

  // Re-arrange disk blocks
  private val part1FreeDiskBlocks: ArrayBuffer[Int] = raw"""\.""".r
    .findAllMatchIn(diskBlocks.map(_.substring(0, 1)).mkString(""))
    .map(_.start)
    .to(ArrayBuffer)
  private val part2FreeDiskBlocks = part1FreeDiskBlocks.clone()

  private val part1Blocks = diskBlocks.clone()

  // Part 1
  breakable {
    for(i <- part1Blocks.length - 1 to 0 by -1) {
      val block = part1Blocks(i)
      if(part1FreeDiskBlocks.isEmpty) break
      val nextFreeBlockIndex = part1FreeDiskBlocks.head
      if(block != "." && i > nextFreeBlockIndex) {
        part1Blocks(nextFreeBlockIndex) = block
        part1FreeDiskBlocks.remove(0)
        part1Blocks(i) = "."
      }
    }
  }

  // Part 2
  private val part2Blocks: ArrayBuffer[String] = diskBlocks.clone()
  // Group freeIndexes into groups of sequential indexes
  private val groupedFreeDiskBlocks: ArrayBuffer[ArrayBuffer[Int]] = getGroupedIndexes(part2FreeDiskBlocks)
  // Group blocks into groups of sequential indexes
  private val groupedBlocks: ArrayBuffer[ArrayBuffer[Int]] = getGroupedBlockIndexes(
    part2Blocks.zipWithIndex
      .filter(_._1 != ".")
  )

  breakable {
    for( i <- groupedBlocks.length - 1 to 0 by -1) {
      val block = groupedBlocks(i)
      if(groupedFreeDiskBlocks.isEmpty) break
      if(groupedFreeDiskBlocks.head(0) >= block(0)) break
      breakable {
        val freeBlock = groupedFreeDiskBlocks.find(freeBlock => {
          freeBlock.length >= block.length
        })
        if(freeBlock.isEmpty) break
        if(freeBlock.get(0) >= block(0)) break
        for(j <- block.indices) {
          val freeIndex = freeBlock.get(j)
          val fromIndex = block(j)
          part2Blocks(freeIndex) = part2Blocks(fromIndex)
          part2Blocks(fromIndex) = "."
        }
        val fbIndex = groupedFreeDiskBlocks.indexOf(freeBlock.get)
        if(freeBlock.get.length == block.length) {
          groupedFreeDiskBlocks.remove(fbIndex)
        } else {
          groupedFreeDiskBlocks(fbIndex) = freeBlock.get.drop(block.length)
        }
      }
    }
  }

  // Print the results
  println(s"Part 1 checksum: ${checksum(part1Blocks.toArray)}")
  println(s"Part 2 checksum: ${checksum(part2Blocks.toArray)}")

  private def checksum(blocks: Array[String]): BigInt = {
    var checksum: BigInt = BigInt(0)
    for(i <- 1 until blocks.length) {
      val block = blocks(i)
      if(block != ".") checksum += BigInt(i) * BigInt(block)
    }
    checksum
  }

  private def getGroupedIndexes(indexes: ArrayBuffer[Int]): ArrayBuffer[ArrayBuffer[Int]] = {
    val result = ArrayBuffer[ArrayBuffer[Int]]()
    while (indexes.nonEmpty) {
      val group = ArrayBuffer[Int](indexes.head)
      indexes.remove(0)
      while (indexes.nonEmpty && Math.abs(indexes.head - group.last) == 1) {
        group += indexes.head
        indexes.remove(0)
      }
      result += group
    }
    result
  }

  private def getGroupedBlockIndexes(indexes: ArrayBuffer[(String, Int)]): ArrayBuffer[ArrayBuffer[Int]] = {
    val result = ArrayBuffer[ArrayBuffer[Int]]()
    while (indexes.nonEmpty) {
      val group = ArrayBuffer[(String, Int)](indexes.head)
      indexes.remove(0)
      while (indexes.nonEmpty && indexes.head._1 == group.last._1) {
        group += indexes.head
        indexes.remove(0)
      }
      result += group.map(_._2)
    }
    result
  }

  // Stop the Spark session
  spark.stop()
}
