package com.ofco.spark.examples.scala.core

import com.ofco.spark.examples.scala.test.SparkTestUtils
import org.scalatest.{BeforeAndAfter, FunSuite}

class WordCountTest extends FunSuite with BeforeAndAfter {

  test("main") {
    val input_file = "data/com/ofco/spark/examples/scala/core/data.txt"
    val output_dir = "tmp/output"

    WordCount.main(Array(input_file, output_dir))
  }

  before {
    SparkTestUtils.initTestEnv()
  }
}