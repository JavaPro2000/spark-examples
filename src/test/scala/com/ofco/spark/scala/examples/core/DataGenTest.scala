package com.ofco.spark.scala.examples.core

import com.ofco.spark.scala.examples.test.SparkTestUtils
import org.scalatest.{BeforeAndAfter, FunSuite}

class DataGenTest extends FunSuite with BeforeAndAfter {

    test("main") {
      val output: String = "tmp/output"
      val numberOfkeys: Int = 1000000
      val numPartitions: Int = 100
      val recordSize: Int = 1024

      DataGen.main(Array(output, numberOfkeys.toString, numPartitions.toString, recordSize.toString))
    }

    before {
      SparkTestUtils.initTestEnv()
    }
}