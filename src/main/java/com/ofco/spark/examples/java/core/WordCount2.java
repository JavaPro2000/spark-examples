package com.ofco.spark.examples.java.core;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;
import java.util.regex.Pattern;

import static com.ofco.spark.examples.java.util.HDFSUtils.deleteFile;
import static com.ofco.spark.examples.java.util.SparkUtils.*;
import static com.ofco.spark.examples.java.util.Utils.getParams;

public class WordCount2 {
    public static void main(String[] args) {
        try {
            Map<String, String> params = getParams(args);
            params.put("app_name", "WordCount");

            start(params);

            run(params);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            stop();
        }
    }

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void run(Map<String, String> params) throws Exception {
        String input = params.get("input");
        String output = params.get("output");

        deleteFile(output);

        SparkSession spark = getSpark();

        JavaRDD<String> lines = spark.read().textFile(input).javaRDD();
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((a, b) -> a + b);

        counts.saveAsTextFile(params.get("output"));
    }
}
