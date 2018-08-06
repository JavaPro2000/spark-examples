package com.ofco.spark.examples.scala.test

import com.ofco.spark.examples.scala.util.Utils
import org.apache.commons.lang3.SystemUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

object SparkTestUtils {
  def initTestEnv(): Unit = {
    val userDir: String = System.getProperty("user.dir").replace('\\', '/')
    val tmpDir: String = s"$userDir/tmp/tmp-" + System.nanoTime()
    val cores: Int = Runtime.getRuntime.availableProcessors

    //sys.props.put("log4j.configuration", "file://" + userDir + "/conf/log4j.properties")
    sys.props.put("hadoop.home.dir", userDir + "/hadoop")
    sys.props.put("hive.exec.scratchdir", tmpDir)
    sys.props.put("java.io.tmpdir", tmpDir + "/spark/tmp")
    sys.props.put("spark.sql.warehouse.dir", tmpDir + "/spark/spark-hive")
    sys.props.put("spark.local.dir", tmpDir + "/spark/scratch")
    sys.props.put("spark.master", "local[" + cores + "]")
    sys.props.put("spark.sql.shuffle.partitions", "" + cores)
    sys.props.put("derby.system.home", tmpDir + "/derby")

    initTmpFolder(tmpDir)
  }

  private def initTmpFolder(tmpDir: String): Unit = {
    val fs: FileSystem = FileSystem.get(new Configuration)
    Utils.deleteDir(fs, tmpDir)
    Utils.makeDir(fs, tmpDir)

    var line: String = s"chmod -R 777 $tmpDir"

    if (SystemUtils.IS_OS_WINDOWS) {
      val userDir: String = System.getProperty("user.dir").replace('\\', '/')
      line = s"$userDir/hadoop/bin/winutils.exe $line"
    }

    if (Utils.executeCommandLine(line) != 0)
      throw new Exception(s"Error while executing chmod on tmp folder => $tmpDir")
  }
}