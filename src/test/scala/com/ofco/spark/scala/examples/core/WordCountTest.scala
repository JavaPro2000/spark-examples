package com.ofco.spark.scala.examples.core

import com.ofco.spark.scala.examples.test.SparkTestUtils
import org.scalatest.{BeforeAndAfter, FunSuite}

class WordCountTest extends FunSuite with BeforeAndAfter{

  test("testMain") {
    val input_file = "data/data.txt"
    val output_dir = "tmp/output"

    WordCount.main(Array(input_file, output_dir))
  }

  before {
    SparkTestUtils.initTestEnv()
  }
}
