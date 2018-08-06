package com.ofco.spark.examples.scala.core

import com.ofco.spark.examples.scala.test.SparkTestUtils
import org.scalatest.{BeforeAndAfter, FunSuite}

class DataGenTest extends FunSuite with BeforeAndAfter {

    test("main") {
      val output: String = "tmp/output"
      val numberOfkeys: Int = 100000
      val numPartitions: Int = 16
      val recordSize: Int = 1024

      DataGen.main(Array(output, numberOfkeys.toString, numPartitions.toString, recordSize.toString))
    }

    before {
      SparkTestUtils.initTestEnv()
    }
}