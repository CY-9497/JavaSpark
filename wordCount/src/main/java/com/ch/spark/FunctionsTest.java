package com.ch.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class FunctionsTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("functionsTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> list = Arrays.asList("a1", "b1", "c1", "d1", "e1");

        JavaRDD<String> parallelize = sc.parallelize(list);
        List<Tuple2<String, String>> list1 = Arrays.asList(new Tuple2<String, String>("zhangSan", "1"), new Tuple2<String, String>("liSi", "2"), new Tuple2<String, String>("wanWu", "3"), new Tuple2<String, String>("maLiu", "4"));
        List<Tuple2<String, String>> list3 = Arrays.asList(new Tuple2<String, String>("zhangSan", "1"), new Tuple2<String, String>("liSi", "4"), new Tuple2<String, String>("wanWu", "5"), new Tuple2<String, String>("aQi", "4"));
        List<Tuple2<String, Integer>> list2 = Arrays.asList(new Tuple2<String, Integer>("zhangSan", 2), new Tuple2<String, Integer>("liSi", 1), new Tuple2<String, Integer>("wanWu", 3), new Tuple2<String, Integer>("aQi", 5));
        JavaPairRDD<String, String> rdd1 = sc.parallelizePairs(list1);
        JavaPairRDD<String, String> rdd3 = sc.parallelizePairs(list3);
        JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(list2);

//        join算子,按两个rdd的key关联,分区于父RDD分区最多的那个一致
        JavaPairRDD<String, Tuple2<String, Integer>> join = rdd1.join(rdd2);
        join.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Integer>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<String, Integer>> stringTuple2Tuple2) throws Exception {
                System.out.println(stringTuple2Tuple2);
            }
        });

//        leftOuterJoin
        JavaPairRDD<String, Tuple2<String, Optional<Integer>>> join1 = rdd1.leftOuterJoin(rdd2);
        join1.foreach(new VoidFunction<Tuple2<String, Tuple2<String, Optional<Integer>>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<String, Optional<Integer>>> stringTuple2Tuple2) throws Exception {
                String key = stringTuple2Tuple2._1;
                String values = stringTuple2Tuple2._2._1;
                Optional<Integer> optional = stringTuple2Tuple2._2._2;
                if(optional.isPresent()){
                    System.out.println("key=" + key + " values=" + values + " optional=" + optional.get());
                }else {
                    System.out.println("key=" + key + " values=" + values + " optional=NULL");
                }
            }
        });

//        rightOuterJoin
        JavaPairRDD<String, Tuple2<Optional<String>, Integer>> join2 = rdd1.rightOuterJoin(rdd2);
        join2.foreach(new VoidFunction<Tuple2<String, Tuple2<Optional<String>, Integer>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Optional<String>, Integer>> stringTuple2Tuple2) throws Exception {
                System.out.println(stringTuple2Tuple2);
            }
        });
//        fullOuterJoin
        JavaPairRDD<String, Tuple2<Optional<String>, Optional<Integer>>> join3 = rdd1.fullOuterJoin(rdd2);
        join3.foreach(new VoidFunction<Tuple2<String, Tuple2<Optional<String>, Optional<Integer>>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Optional<String>, Optional<Integer>>> stringTuple2Tuple2) throws Exception {
                System.out.println(stringTuple2Tuple2);
            }
        });

//        union
        JavaPairRDD<String, String> union = rdd1.union(rdd3);
        union.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
                System.out.println(stringStringTuple2);
            }
        });

//        intersection取交集
        JavaPairRDD<String, String> intersection = rdd1.intersection(rdd3);
        intersection.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
                System.out.println(stringStringTuple2);
            }
        });

//        subtract取差集
        JavaPairRDD<String, String> subtract = rdd1.subtract(rdd3);
        subtract.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
                System.out.println(stringStringTuple2);
            }
        });

//        cogroup将两个RDD的key合并，一个RDD对应一个value
        JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> cogroup = rdd1.cogroup(rdd3);
        cogroup.foreach(new VoidFunction<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>> stringTuple2Tuple2) throws Exception {
                System.out.println(stringTuple2Tuple2);
            }
        });

//        foreachPartition
        parallelize.foreachPartition(new VoidFunction<Iterator<String>>() {
            public void call(Iterator<String> s) throws Exception {
                List<String> strings = new ArrayList<>();
                System.out.println("创建数据库连接.。。");
                while (s.hasNext()){
                    String s1 = s.next();
                    strings.add(s1);
                }
                System.out.println("关闭数据库");
            }
        });

        sc.stop();

    }
}
