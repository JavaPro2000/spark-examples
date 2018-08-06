package com.ofco.spark.examples.scala.core

import com.ofco.spark.examples.scala.test.SparkTestUtils
import org.scalatest.{BeforeAndAfter, FunSuite}

class SparkPiTest extends FunSuite with BeforeAndAfter {

  test("main") {
    val slices: Int = 10

    SparkPi.main(Array(slices.toString))
  }

  before {
    SparkTestUtils.initTestEnv()
  }
}