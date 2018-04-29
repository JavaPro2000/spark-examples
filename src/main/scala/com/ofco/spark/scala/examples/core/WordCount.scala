package com.ofco.spark.scala.examples.core

import com.ofco.spark.scala.examples.util.SparkUtils.{init, tryWithResource}
import com.ofco.spark.scala.examples.util.HDFSUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: <input file> <output file>")
      System.exit(1)
    }

    val input: String = args(0)
    val output: String = args(1)

    tryWithResource(init("Spark Scala WordCount")) {
      spark => run(spark, input, output)
    }
  }

  def run(spark: SparkSession, input: String, output: String): Unit = {
    val lines: RDD[String] = read(spark, input)
    val counts: RDD[(String, Long)] = process(lines)
    write(counts, output)
  }

  def read(spark: SparkSession, input: String): RDD[String] = {
    spark.sparkContext.textFile(input)
  }

  def process(lines: RDD[String]): RDD[(String, Long)] = {
    lines.flatMap(line => line.split(' ')) //Get words
      .map(word => (word, 1l)) // Get ones
      .reduceByKey((a, b) => a + b) // Get counts
  }

  def write(counts: RDD[(String, Long)], output: String): Unit = {
    HDFSUtils.deleteFolder(output)
    counts.saveAsTextFile(output)
  }
}