package com.ch.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class ParallelizeTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("parallelizeTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> list = Arrays.asList(
                "love1","love2","love3","love4",
                "love5","love6","love7","love8",
                "love9","love10","love11","love12","love13"
        );

//        parallelize
//        JavaRDD<String> rdd = sc.parallelize(list);
//        System.out.println("rdd parallelize length:" + rdd.partitions().size());
//        List<Tuple2<String, Integer>> list1 = Arrays.asList(new Tuple2<String, Integer>("zhangSan", 1), new Tuple2<String, Integer>("liSi", 2), new Tuple2<String, Integer>("wanWu", 3));
//        List<Tuple2<String, String>> list2 = Arrays.asList(new Tuple2<String, String>("zhangSan", "1"), new Tuple2<String, String>("liSi", "2"), new Tuple2<String, String>("wanWu", "3"));
//        JavaRDD<Tuple2<String, Integer>> rdd1 = sc.parallelize(list1);
//        JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(list1);

//        List<String> collect = rdd.collect();
        JavaRDD<String> rdd = sc.parallelize(list,3);
        JavaRDD<String> stringJavaRDD = rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer integer, Iterator<String> stringIterator) throws Exception {
                List<String> list1 = new ArrayList<>();
                while (stringIterator.hasNext()) {
                    String one = stringIterator.next();
                    list1.add("partition index = [" + integer + "],value = [" + one + "]");
                }
                return list1.iterator();
            }
        },true);
        List<String> collect = stringJavaRDD.collect();
        collect.forEach(System.out::println);

//      repartition,是有shuffle的算子,可以对RDD重新分区,可以增加分区也可以减少分区
//        JavaRDD<String> rdd1 = stringJavaRDD.repartition(4);

//        coalesce和repartition一样可以对RDD重新分区,可以增加分区也可以减少分区
//        coalesce(slice,shuffle[boolean = false] false不产生shuffle
        JavaRDD<String> rdd1 = stringJavaRDD.coalesce(2);
        JavaRDD<String> stringJavaRDD1 = rdd1.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer integer, Iterator<String> stringIterator) throws Exception {
                List<String> list1 = new ArrayList<>();
                while (stringIterator.hasNext()) {
                    String one = stringIterator.next();
                    list1.add("partition index = [" + integer + "],value = [" + one + "]");
                }
                return list1.iterator();
            }
        }, true);
        List<String> collect1 = stringJavaRDD1.collect();
        collect1.forEach(System.out::println);

        sc.stop();
    }
}
