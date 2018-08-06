package com.ofco.spark.examples.scala.core

import com.ofco.spark.examples.scala.util.HDFSUtils
import com.ofco.spark.examples.scala.util.SparkUtils.{init, tryWithResource}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.Random

object DataGen {

  case class Record(i: Long, s: String)

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: <output file> <number of keys> <number of partitions> <record size>")
      System.exit(1)
    }

    val output: String = args(0)
    val numberOfKeys: Int = args(1).toInt
    val numPartitions: Int = args(2).toInt
    val recordSize: Int = args(3).toInt

    tryWithResource(init("DataGen")) {
      spark => run(spark, output, numberOfKeys, numPartitions, recordSize)
    }
  }

  def createString(recordSize: Int): String = {
    Random.alphanumeric.take(recordSize).mkString
  }

  def run(spark: SparkSession, output: String, numberOfKeys: Int = 100, numPartitions: Int = 1, recordSize: Int = 1024): Unit = {
    val keysRDD: RDD[Record] = spark.sparkContext.parallelize(1 until numberOfKeys + 1, numPartitions).map(id => Record(id, createString(recordSize)))

    import spark.implicits._

    val keysDS: Dataset[Record] = spark.createDataset(keysRDD)

    HDFSUtils.deleteFolder(output)
    keysDS.write.csv(output)
  }

  def run2(spark: SparkSession, output: String, numberOfKeys: Int = 100, numPartitions: Int = 1, recordSize: Int = 1024): Unit = {
    val keysRDD: RDD[String] = spark.sparkContext.parallelize(1 until numberOfKeys + 1, numPartitions).map(id => id + "," + createString(recordSize))
    HDFSUtils.deleteFolder(output)
    keysRDD.saveAsTextFile(output)
  }
}
