package com.ofco.spark.examples.java.core;

import org.junit.BeforeClass;
import org.junit.Test;

import static com.ofco.spark.examples.java.test.SparkTestUtils.initSparkTestEnv;

public class WordCount2Test {
    @Test
    public void testMain() {
        WordCount2.main(new String[]{"--input", "data/com/ofco/spark/examples/java/core/data.txt", "--output", "tmp/output/"});
    }

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        initSparkTestEnv();
    }
}
