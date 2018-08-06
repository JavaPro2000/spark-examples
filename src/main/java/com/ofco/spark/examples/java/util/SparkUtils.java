package com.ofco.spark.examples.java.util;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

public class SparkUtils {
	private static SparkSession spark;

	public static SparkSession getSpark() {
		return spark;
	}

	public static void start(Map<String, String> params) {
		SparkConf conf = new SparkConf();

		String appName = params.get("app_name");
		if (appName != null && appName.trim().length() > 0) {
			conf.setAppName("appName");
		}

		String master = params.get("master");
		if (master != null && master.trim().length() > 0) {
			conf.setMaster(master);
		}

		spark = SparkSession.builder().config(conf).getOrCreate();
	}

	public static void stop() {
		if (spark != null) {
			spark.stop();
		}
	}
}
