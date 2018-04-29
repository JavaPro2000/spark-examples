package com.ofco.spark.scala.examples.core

import com.ofco.spark.scala.examples.test.SparkTestUtils
import org.scalatest.{BeforeAndAfter, FunSuite}

class ScalaWordCountTest extends FunSuite with BeforeAndAfter{

  test("testMain") {
    val input_file = "data/data.txt"
    val output_dir = "tmp/output"

    ScalaWordCount.main(Array(input_file, output_dir))
  }

  before {
    SparkTestUtils.initTestEnv()
  }
}
