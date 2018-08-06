package com.ofco.spark.examples.java.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HDFSUtils {
	public static void deleteFile(String filename) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		Path path = new Path(filename);
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
	}
}
