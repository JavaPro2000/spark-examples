package com.ofco.spark.examples.java.test;

public class SparkTestUtils {
	public static void initSparkTestEnv() {
		String userDir = System.getProperty("user.dir").replace('\\', '/');
		int cores = Runtime.getRuntime().availableProcessors();
		String sparkHiveWarehouseDir = "file:///" + userDir + "/spark-hive";

		System.setProperty("hive.exec.scratchdir", userDir + "/tmp");
		System.setProperty("hive.metastore.warehouse.dir", sparkHiveWarehouseDir);
		System.setProperty("java.io.tmpdir", userDir + "/tmp/spark/tmp");
		System.setProperty("hadoop.home.dir", userDir + "/hadoop");
		System.setProperty("spark.sql.shuffle.partitions", "" + cores);
		System.setProperty("spark.local.dir", userDir + "/tmp/spark/scratch");
		System.setProperty("spark.master", "local[*]");
		System.setProperty("spark.driver.extraJavaOptions", "-Dderby.system.home=" + userDir + "/tmp/derby");

		//System.setProperty("log4j.configuration", "file:///" + userDir + "/conf/log4j.properties");
	}
}
