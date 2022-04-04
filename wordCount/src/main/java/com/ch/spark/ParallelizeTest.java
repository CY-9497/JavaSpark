package com.ch.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class ParallelizeTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("parallelizeTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> list = Arrays.asList("a", "b", "c", "d", "e");
        JavaRDD<String> rdd = sc.parallelize(list);
        System.out.println("rdd parallelize length:" + rdd.partitions().size());
        List<String> collect = rdd.collect();
        sc.stop();
    }
}
