package com.ofco.spark.scala.examples.core

import com.ofco.spark.scala.examples.test.SparkTestUtils
import org.scalatest.{BeforeAndAfter, FunSuite}

class SparkPiTest extends FunSuite with BeforeAndAfter {

  test("testMain") {
    val slices: Int = 10

    SparkPi.main(Array(slices.toString))
  }

  before {
    SparkTestUtils.initTestEnv()
  }
}