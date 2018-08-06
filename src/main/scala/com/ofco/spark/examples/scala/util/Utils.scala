package com.ofco.spark.examples.scala.util

import java.io.IOException

import org.apache.commons.exec.{CommandLine, DefaultExecutor}
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.io.Source.fromFile

object Utils {
  @throws[IOException]
  def deleteDir(fs: FileSystem, dir: String): Unit = {
    val path = new Path(dir)
    if (fs.exists(path)) fs.delete(path, true)
  }

  @throws[IOException]
  def makeDir(fs: FileSystem, dir: String): Unit = {
    val path = new Path(dir)
    if (!fs.mkdirs(path)) throw new IOException(s"Error while creating folder => $dir")
  }

  @throws[IOException]
  def executeCommandLine(line: String): Int = {
    val commandLine = CommandLine.parse(line)
    val executor = new DefaultExecutor
    executor.execute(commandLine)
  }

  def readFile(filename: String): List[String] = {
    val lineIter: Iterator[String] = fromFile(filename).getLines()
    val lineList: List[String] = lineIter.toList
    lineList
  }
}